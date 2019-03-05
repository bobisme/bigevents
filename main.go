package main

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"cloud.google.com/go/bigtable"
	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"google.golang.org/api/option"
)

const globalStream = "idx|__global"
const globalPosKey = "pos|__global"
const family = "x"
const RFC3339Mili = "2006-01-02T15:04:05.999Z07:00"

type devTokenSource struct{}

func (devTokenSource) Token() (*oauth2.Token, error) {
	return new(oauth2.Token), nil
}

func NewDevBigTableClient() (*bigtable.Client, error) {
	ctx := context.Background()
	project := "dev"
	instance := "dev"
	return bigtable.NewClient(
		ctx,
		project,
		instance,
		option.WithTokenSource(&devTokenSource{}),
	)
}

func panicIf(err error) {
	if err != nil {
		panic(err)
	}
}

func key_(parts ...string) string {
	return strings.Join(parts, "|")
}

type Message struct {
	Stream    string `json:"stream"`
	Version   int64  `json:"version"`
	Position  int64  `json:"position"`
	DataBytes []byte `json:"dataBytes"`
	MetaBytes []byte `json:"metaBytes"`
	rowKey    string
	// Data           interface{} `json:"data"`
	// Meta           interface{} `json:"meta"`
}

func (m *Message) String() string {
	out, err := json.Marshal(m)
	panicIf(err)
	return string(out)
}

func streamKey(streamName string, position int64) string {
	return fmt.Sprintf("msg|%s|%s", streamName, int64ToHex(position))
}

type PublishFn func(stream string, data, meta interface{}, expectedVer *int64)

func hexToInt64(h string) (int64, error) {
	b, err := hex.DecodeString(h)
	if err != nil {
		return 0, err
	}
	return int64(binary.BigEndian.Uint64(b)), nil
}

func int64ToHex(pos int64) string {
	return fmt.Sprintf("%016x", pos)
}

func int64ToByte(pos int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(pos))
	return b
}

func byteToInt64(pos []byte) int64 {
	return int64(binary.BigEndian.Uint64(pos))
}

func streamCategory(stream string) string {
	parts := strings.SplitN(stream, "-", 2)
	return parts[0]
}

func NewPublishFn(ctx context.Context, table *bigtable.Table) PublishFn {
	lastStreamPosPrefix := func(stream string) string {
		return "idx|lastPos|" + stream + "|"
	}
	lastStreamPosKey := func(stream string, pos int64) string {
		return lastStreamPosPrefix(stream) + int64ToHex((1<<63-1)-pos)
	}
	lastStreamPos := func(stream string) int64 {
		var rowKey string
		table.ReadRows(ctx,
			bigtable.PrefixRange(lastStreamPosPrefix(stream)),
			func(row bigtable.Row) bool {
				rowKey = row.Key()
				return false
			},
			bigtable.RowFilter(bigtable.LatestNFilter(1)),
			bigtable.LimitRows(1),
		)
		if rowKey == "" {
			return 0
		}
		parts := strings.Split(rowKey, "|")
		if len(parts) < 4 {
			log.Panic().Str("key", rowKey).
				Msg("last position key didn't have enough parts")
		}
		pos, err := hexToInt64(parts[3])
		if err != nil {
			log.Panic().Str("key", rowKey).Err(err)
		}
		truePos := (1<<63 - 1) - pos
		return truePos
	}

	nextGlobalPos := func() int64 {
		rmw := bigtable.NewReadModifyWrite()
		rmw.Increment(family, "lastPos", 1)
		r, err := table.ApplyReadModifyWrite(ctx, globalPosKey, rmw)
		panicIf(err)
		items := r[family]
		col := items[len(items)-1]
		return int64(binary.BigEndian.Uint64(col.Value))
	}

	applyIfNotExists := func(key string, mut *bigtable.Mutation) (ok bool, err error) {
		var mutRes bool
		cond := bigtable.NewCondMutation(bigtable.LatestNFilter(1), nil, mut)
		err = table.Apply(
			ctx, key, cond, bigtable.GetCondMutationResult(&mutRes))
		return !mutRes, err
	}

	reserveGlobalPos := func(stream string) int64 {
		lastPos := lastStreamPos("__global")
		mut := bigtable.NewMutation()
		mut.Set(family, "stream", bigtable.Now(), []byte(stream))
		for attempt := int64(1); attempt <= 10000; attempt++ {
			pos := lastPos + attempt
			key := key_(globalStream, int64ToHex(pos))
			ok, err := applyIfNotExists(key, mut)
			if err != nil {
				log.Error().Err(err)
				return 0
			}
			if ok {
				log.Debug().Int64("pos", pos).Msg("reserved global position")
				return pos
			}
			log.Warn().Int64("pos", pos).Msg("couldn't reserve global position")
			// time.Sleep(20 * time.Millisecond)
		}
		log.Error().Int("maxAttempts", 10000).
			Msg("exceeded max attempts to reserve position")
		return 0
	}

	releaseGlobalPosition := func(pos int64) {
		del := bigtable.NewMutation()
		del.DeleteRow()
		key := key_(globalStream, int64ToHex(pos))
		table.Apply(ctx, key, del)
	}

	writeMessageIfNotExists := func(
		key string, data, meta []byte, ver, pos int64) (didWrite bool, err error) {
		//
		mut := bigtable.NewMutation()
		mut.Set(family, "data", bigtable.Now(), data)
		mut.Set(family, "meta", bigtable.Now(), meta)
		mut.Set(family, "ver", bigtable.Now(), int64ToByte(ver))
		mut.Set(family, "pos", bigtable.Now(), int64ToByte(pos))
		return applyIfNotExists(key, mut)
	}

	mustWriteMessageV0 := func(stream string, data, meta []byte) (
		globalPos, streamVer int64, err error) {
		var ok bool
		attempt := 1
		maxAttempts := 1000
		streamVer = lastStreamPos(stream)
		for {
			streamVer++
			globalPos = nextGlobalPos()
			ok, err = writeMessageIfNotExists(
				streamKey(stream, streamVer), data, meta, streamVer, globalPos)
			if ok {
				log.Debug().
					Str("stream", stream).
					Int64("pos", streamVer).
					Msg("wrote message to stream position")
				return
			}
			if err != nil {
				return
			}
			log.Warn().
				Str("stream", stream).
				Int64("pos", streamVer).
				Msg("could not write message to stream position")
			attempt++
			if attempt > maxAttempts {
				err = fmt.Errorf(
					"exceeded max attempts to write message: %d", maxAttempts)
				return
			}
		}
	}
	mustWriteMessageV1 := func(stream string, data, meta []byte) (
		globalPos, streamVer int64, err error) {
		var ok bool
		streamVer = lastStreamPos(stream)
		for i := 1; i < 1000; i++ {
			streamVer++
			globalPos = reserveGlobalPos(stream)
			ok, err = writeMessageIfNotExists(
				streamKey(stream, streamVer), data, meta, streamVer, globalPos)
			if ok {
				log.Debug().
					Str("stream", stream).
					Int64("ver", streamVer).
					Msg("wrote message to stream position")
				return
			}
			releaseGlobalPosition(globalPos)
			if err != nil {
				return
			}
			log.Warn().
				Str("stream", stream).
				Int64("ver", streamVer).
				Msg("could not write message to stream position")
		}
		log.Error().Int("maxAttempts", 10000).
			Msg("exceeded max attempts to reserve position")
		return
	}

	// writeCategoryMessage := func(
	// 	pos int64, category, streamKey string, data, meta []byte) error {
	// 	//
	// 	key := "cat|" + category + "|" + int64ToHex(pos) + "|" + streamKey
	// 	_, err := writeMessageIfNotExists(key, data, meta, 0, pos)
	// 	return err
	// }

	writeGlobalPos := func(pos int64, streamKey string) error {
		mut := bigtable.NewMutation()
		mut.Set(family, "key", bigtable.Now(), []byte(streamKey))
		return table.Apply(ctx, globalStream+"|"+int64ToHex(pos), mut)
	}

	writeLastStreamPos := func(stream string, pos int64) error {
		mut := bigtable.NewMutation()
		mut.Set(family, "_", bigtable.Now(), []byte{})
		return table.Apply(ctx, lastStreamPosKey(stream, pos), mut)
	}

	_ = mustWriteMessageV0
	// _ = writeCategoryMessage
	_ = writeGlobalPos
	_ = writeLastStreamPos
	return func(stream string, data, meta interface{}, expectedVer *int64) {
		encodedData, err := json.Marshal(data)
		panicIf(err)
		encodedMeta, err := json.Marshal(meta)
		panicIf(err)

		var globalPos, streamVer int64
		if expectedVer == nil {
			globalPos, streamVer, err = mustWriteMessageV1(
				stream, encodedData, encodedMeta)
		} else {
			globalPos = reserveGlobalPos(stream)
			ok, err := writeMessageIfNotExists(
				streamKey(stream, streamVer), encodedData, encodedMeta,
				streamVer, globalPos)
			if !ok || err != nil {
				releaseGlobalPosition(globalPos)
				if !ok {
					panic("could not write version because it exists")
				}
				if err != nil {
					panic(err)
				}
			}
		}

		msgMut := bigtable.NewMutation()
		msgMut.Set(family, "data", bigtable.Now(), encodedData)
		msgMut.Set(family, "meta", bigtable.Now(), encodedMeta)
		msgMut.Set(family, "pos", bigtable.Now(), int64ToByte(globalPos))
		msgMut.Set(family, "ver", bigtable.Now(), int64ToByte(streamVer))
		msgMut.Set(family, "key", bigtable.Now(), []byte(streamKey(stream, streamVer)))
		emptyMut := bigtable.NewMutation()
		emptyMut.Set(family, "_", bigtable.Now(), []byte{})
		table.ApplyBulk(ctx, []string{
			key_(globalStream, int64ToHex(globalPos), stream),
			lastStreamPosKey(stream, streamVer),
			lastStreamPosKey("__global", globalPos),
		}, []*bigtable.Mutation{msgMut, emptyMut, emptyMut})
		// return table.Apply(ctx, lastStreamPosKey(stream, pos), mut)

		// mutLastGlobalPos := bigtable.NewMutation()
		// err = writeCategoryMessage(
		// 	globalPos, streamCategory(stream), streamKey(stream, streamVer),
		// 	encodedData, encodedMeta)
		// panicIf(err)
		// err = writeGlobalPos(globalPos, streamKey(stream, streamVer))
		// panicIf(err)
		// err = writeLastStreamPos(stream, streamVer)
		// panicIf(err)
		// err = writeLastStreamPos("__global", globalPos)
		// panicIf(err)
	}
}

type ReadOptions struct {
	rowLimit int
}

func newReadOptions() ReadOptions {
	return ReadOptions{
		rowLimit: 1000,
	}
}
func LimitRows(n int) ReadOptions { return ReadOptions{rowLimit: n} }

type ReadStreamFn func(stream string, globalStartPos int64, opts ...ReadOptions) ([]*Message, error)
type ReadCategoryFn func(cat string, startPos int64, opts ...ReadOptions) ([]*Message, error)

// valuePattern := `^msg\|` + stream + `\|`
func firstStreamKey(
	ctx context.Context, table *bigtable.Table,
	valuePattern string, globalStartPos int64) (string, error) {
	//
	var firstRow bigtable.Row
	err := table.ReadRows(ctx,
		bigtable.InfiniteRange(globalStream+"|"+int64ToHex(globalStartPos)),
		func(row bigtable.Row) bool {
			firstRow = row
			return false
		},
		bigtable.RowFilter(
			bigtable.ChainFilters(
				bigtable.LatestNFilter(1),
				bigtable.ValueFilter(valuePattern))),
		bigtable.LimitRows(1),
	)
	if err != nil {
		return "", err
	}
	if firstRow == nil {
		return "", nil
	}
	return string(firstRow[family][0].Value), nil
}

func streamNameFromKey(key string) string {
	parts := strings.Split(key, "|")
	if len(parts) >= 2 && parts[0] == "msg" {
		return parts[1]
	}
	if len(parts) >= 4 && parts[1] == "__global" {
		return parts[3]
	}
	if len(parts) >= 5 && parts[0] == "cat" {
		return parts[4]
	}
	return ""
}

func rowToMessage(row bigtable.Row) (*Message, error) {
	var err error
	msg := &Message{
		rowKey: row.Key(), // DEBUG
		Stream: streamNameFromKey(row.Key()),
	}
	for _, col := range row[family] {
		switch col.Column {
		case family + ":data":
			msg.DataBytes = col.Value
		case family + ":meta":
			msg.MetaBytes = col.Value
		case family + ":pos":
			msg.Position = byteToInt64(col.Value)
		case family + ":ver":
			msg.Version = byteToInt64(col.Value)
		}
	}
	parts := strings.Split(row.Key(), "|")
	if len(parts) >= 3 && parts[0] == "msg" {
		msg.Version, err = hexToInt64(parts[2])
		if err != nil {
			return nil, err
		}
	} else if len(parts) >= 6 && parts[0] == "cat" {
		msg.Version, err = hexToInt64(parts[5])
		if err != nil {
			return nil, err
		}
	}
	return msg, nil
}

func readStreamPattern(ctx context.Context, table *bigtable.Table,
	pattern string, startPos int64,
	opts ...ReadOptions) ([]*Message, error) {
	//
	finalOpts := newReadOptions()
	for _, opt := range opts {
		if opt.rowLimit > 0 {
			finalOpts.rowLimit = opt.rowLimit
		}
	}
	key, err := firstStreamKey(ctx, table, pattern, startPos)
	if err != nil || key == "" {
		return nil, err
	}
	log.Debug().Str("key", key).Msg("scanning")

	messages := make([]*Message, 0, finalOpts.rowLimit)
	var iterErr error
	rowIter := func(row bigtable.Row) bool {
		var msg *Message
		msg, iterErr = rowToMessage(row)
		if iterErr != nil {
			return false
		}
		messages = append(messages, msg)
		return true
	}

	err = table.ReadRows(ctx,
		bigtable.InfiniteRange(key), rowIter,
		bigtable.RowFilter(
			bigtable.ChainFilters(
				bigtable.LatestNFilter(1),
				bigtable.RowKeyFilter(pattern))),
		bigtable.LimitRows(int64(finalOpts.rowLimit)),
	)

	if err != nil {
		return nil, err
	}
	if iterErr != nil {
		return nil, iterErr
	}
	return messages, nil
}

func NewReadStreamFn(ctx context.Context, table *bigtable.Table) ReadStreamFn {
	return func(stream string, startPos int64, opts ...ReadOptions) ([]*Message, error) {
		return readStreamPattern(ctx, table, `^msg\|`+stream+`\|`, startPos, opts...)
	}
}

func NewReadCategoryFn(ctx context.Context, table *bigtable.Table) ReadCategoryFn {
	return func(cat string, startPos int64, opts ...ReadOptions) ([]*Message, error) {
		finalOpts := newReadOptions()
		for _, opt := range opts {
			if opt.rowLimit > 0 {
				finalOpts.rowLimit = opt.rowLimit
			}
		}

		messages := make([]*Message, 0, finalOpts.rowLimit)
		var iterErr error
		rowIter := func(row bigtable.Row) bool {
			hasData := false
			for _, col := range row[family] {
				if col.Column == family+":data" {
					hasData = true
					break
				}
			}

			if !hasData {
				return false
			}

			var msg *Message
			msg, iterErr = rowToMessage(row)
			if iterErr != nil {
				return false
			}
			messages = append(messages, msg)
			return true
		}
		rangeStart := key_(globalStream, int64ToHex(startPos), cat)
		pattern := `^idx\|__global\|[0-9a-f]{16}\|` + cat + `-`
		err := table.ReadRows(ctx,
			bigtable.InfiniteRange(rangeStart), rowIter,
			bigtable.RowFilter(
				bigtable.ChainFilters(
					bigtable.LatestNFilter(1),
					bigtable.RowKeyFilter(pattern))),
			bigtable.LimitRows(int64(finalOpts.rowLimit)),
		)

		if err != nil {
			return nil, err
		}
		if iterErr != nil {
			return nil, iterErr
		}
		return messages, nil
	}
}

func init() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

func main() {
	ctx := context.TODO()
	client, err := NewDevBigTableClient()
	panicIf(err)
	table := client.Open("events")

	_publish := NewPublishFn(ctx, table)
	publish := func(stream string, data, meta interface{}, ver *int64) {
		start := time.Now()
		_publish(stream, data, meta, ver)
		log.Debug().Dur("ms", time.Since(start)).Msg("publish time")
	}

	data := map[string]string{"a": "2"}
	meta := map[string]string{
		"at": time.Now().UTC().Format(RFC3339Mili),
	}
	_, _ = data, meta
	_ = publish
	// go func() { publish("customer-1234", data, meta, nil) }()
	// go func() { publish("customer-1234", data, meta, nil) }()
	// go func() { publish("customer-1234", data, meta, nil) }()
	// for i := 0; i < 1; i++ {
	// 	for j := 0; j < 8; j++ {
	// 		name := fmt.Sprintf("customer-%[1]x%[1]x%[1]x%[1]x", j)
	// 		go func() {
	// 			for k := 0; k < 4; k++ {
	// 				publish(name, data, meta, nil)
	// 			}
	// 		}()
	// 	}
	// }
	// time.Sleep(1000 * time.Millisecond)

	// UTIL
	logMessage := func(msg *Message) {
		log.Info().
			Str("stream", msg.Stream).
			// Str("rowKey", msg.rowKey).
			Int64("v", msg.Version).
			Int64("pos", msg.Position).
			Str("data", string(msg.DataBytes)).
			Str("meta", string(msg.MetaBytes)).
			Msg("read message")
	}

	// READ STREAM
	log.Info().Msg("READING STREAMS")
	readStream := NewReadStreamFn(ctx, table)
	_readStream := readStream
	readStream = func(stream string, pos int64, opts ...ReadOptions) ([]*Message, error) {
		start := time.Now()
		msgs, err := _readStream(stream, pos, opts...)
		log.Info().
			Int("count", len(msgs)).
			Dur("ms", time.Since(start)).
			Msg("read messages")
		return msgs, err
	}

	msgs, err := readStream("customer-1234", 0, LimitRows(5))
	panicIf(err)
	if len(msgs) == 0 {
		log.Error().Msg("no messages found")
	}
	for _, msg := range msgs {
		logMessage(msg)
	}

	// READING A CATEGORY
	log.Info().Msg("READING CATEGORIES")
	readCategory := NewReadCategoryFn(ctx, table)
	_readCategory := readCategory
	readCategory = func(cat string, pos int64, opts ...ReadOptions) ([]*Message, error) {
		start := time.Now()
		msgs, err := _readCategory(cat, pos, opts...)
		log.Info().
			Int("count", len(msgs)).
			Dur("ms", time.Since(start)).
			Msg("read messages")
		return msgs, err
	}
	msgs, err = readCategory("customer", 0)
	panicIf(err)
	if len(msgs) == 0 {
		log.Error().Msg("no messages found")
	}
	for _, msg := range msgs {
		logMessage(msg)
	}

	// DEBUG INFO
	printDebug := func() {
		table.ReadRows(ctx, bigtable.PrefixRange(""), func(row bigtable.Row) bool {
			if strings.HasPrefix(row.Key(), globalStream) {
				l := log.Debug()
				for _, col := range row[family] {
					l = l.Bytes(col.Column, col.Value)
				}
				l.Msg(row.Key())
				return true
			}
			parts := strings.Split(row.Key(), "|")
			if parts[0] == "msg" {
				var data, meta, pos, ver []byte
				for _, col := range row[family] {
					switch col.Column {
					case family + ":data":
						data = col.Value
					case family + ":meta":
						meta = col.Value
					case family + ":pos":
						pos = col.Value
					case family + ":ver":
						ver = col.Value
					default:
						log.Debug().Str("col", col.Column).Bytes("x", col.Value).
							Msg("unknown column")
					}
				}
				log.Debug().
					Str("stream", parts[1]).
					Bytes("ver", ver).
					Bytes("pos", pos).
					Str("data", string(data)).
					Str("meta", string(meta)).
					Msg("msg")
				return true
			}
			for _, col := range row[family] {
				log.Printf("%s = %s: %v", row.Key(), col.Column, col.Value)
			}
			return true
		}, bigtable.RowFilter(bigtable.LatestNFilter(1)))
	}
	_ = printDebug
	// printDebug()
}
