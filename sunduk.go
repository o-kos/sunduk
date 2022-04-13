package sunduk

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/andybalholm/brotli"
	"io"
	"math/rand"
	"os"
	"sort"
	"strings"
)

type entry struct {
	Offset int64
	Size   int64
}

type Sunduk struct {
	FilePath string // FilePath is the path to the file used to persist

	file   *os.File
	offset int64
	data   map[string][]byte
	index  map[string]entry
}

// New creates a new Sunduk
func New(filePath string) *Sunduk {
	store := &Sunduk{
		FilePath: filePath,
		data:     make(map[string][]byte),
		index:    make(map[string]entry),
	}
	err := store.loadFromDisk()
	if err != nil {
		panic(err)
	}
	return store
}

// Close closes the store's file if it isn't already closed.
// Note that any write actions, such as the usage of Put, PutAll or Delete, will automatically re-open the store
func (store *Sunduk) Close() {
	if store.file == nil {
		return
	}
	_ = store.file.Close()
	store.file = nil
}

// Get returns the value of a key as well as a bool that indicates whether an entry exists for that key
func (store *Sunduk) Get(key string) (value []byte, ok bool) {
	value, ok = store.data[key]
	if ok {
		return
	}
	entry, ok := store.index[key]
	if !ok {
		return
	}

	ok = false
	_, err := store.file.Seek(entry.Offset+store.offset, 0)
	if err != nil {
		return
	}

	data := make([]byte, entry.Size)
	data = data[:cap(data)]
	n, err := store.file.Read(data)
	if int64(n) != entry.Size || err != nil {
		return
	}

	var zb bytes.Buffer
	zr := brotli.NewReader(&zb)
	_, _ = zr.Read(data)
	value = zb.Bytes()
	ok = true
	return
}

// Put creates an entry or updates the value of an existing key
func (store *Sunduk) Put(key string, value []byte) error {
	store.index[key] = entry{0, int64(len(value))}
	store.data[key] = value
	return store.flush()
}

// PutAll creates or updates a map of entries
func (store *Sunduk) PutAll(entries map[string][]byte) error {
	for key, value := range entries {
		store.data[key] = value
		store.index[key] = entry{0, 0}
	}
	return store.flush()
}

// Delete removes a key from the store
func (store *Sunduk) Delete(key string) error {
	delete(store.data, key)
	return nil
}

// Count returns the total number of entries in the store
func (store *Sunduk) Count() int {
	length := len(store.data)
	return length
}

// Keys returns a list of all keys
func (store *Sunduk) Keys() []string {
	keys := make([]string, len(store.data))
	i := 0
	for k := range store.data {
		keys[i] = k
		i++
	}
	return keys
}

// loadFromDisk loads the store from the disk and consolidates the entries, or creates an empty file if there is no file
func (store *Sunduk) loadFromDisk() error {
	store.data = make(map[string][]byte)
	file, err := os.Open(store.FilePath)
	if err != nil {
		// Check if the file exists, if it doesn't, then create it and return
		if os.IsNotExist(err) {
			file, err := os.Create(store.FilePath)
			if err != nil {
				return err
			}
			store.file = file
			return nil
		} else {
			return err
		}
	}
	// File doesn't exist, so we need to read it
	store.file = file
	return store.readHeader()
}

// readHeader read, decompress and unmarshall storage header
func (store *Sunduk) readHeader() error {
	makeErr := func(action string, err error) error {
		return fmt.Errorf("unable to %s storage header: %v", action, err)
	}

	// Read compressed header size
	var sb [4]byte
	if n, err := store.file.Read(sb[:]); n != len(sb) || err != nil {
		return makeErr("read size of", err)
	}
	cs := binary.BigEndian.Uint32(sb[:])

	// Read decompressed header size
	if n, err := store.file.Read(sb[:]); n != len(sb) || err != nil {
		return makeErr("read size of", err)
	}
	us := binary.BigEndian.Uint32(sb[:])

	// Read compressed header content
	data := make([]byte, cs)
	if n, err := store.file.Read(data[:]); uint32(n) != cs || err != nil {
		return makeErr("read", err)
	}
	// Save offset of storage data
	store.offset = int64(cs + 2*uint32(len(sb)))

	// Decompress header
	header := make([]byte, us)
	zr := brotli.NewReader(bytes.NewReader(data))
	if n, err := zr.Read(header); uint32(n) != us || err != nil {
		return makeErr("decompress", err)
	}

	// Unmarshall header data
	dec := gob.NewDecoder(bytes.NewBuffer(header))
	if err := dec.Decode(&store.index); err != nil {
		return makeErr("decode", err)
	}

	return nil
}

// flush combines all entries recorded in the file and re-saves only the necessary entries.
// The function is executed on creation, but can also be executed manually if storage space is a concern.
// The original file is backed up
func (store *Sunduk) flush() error {
	// Back up the old file before doing the flushing
	store.Close()
	bakname := store.FilePath + ".bak"
	err := os.Rename(store.FilePath, bakname)
	if err != nil {
		return fmt.Errorf(
			"unable to rename %s to %s during flushing: %s", store.FilePath, bakname, err.Error(),
		)
	}

	err = store.save()
	_ = os.Remove(bakname)
	if err != nil {
		_ = os.Rename(bakname, store.FilePath)
		return fmt.Errorf("unable to save new file at %s during flushing: %s", store.FilePath, err.Error())
	}

	_ = os.Remove(bakname)
	return nil
}

// writeCompressed compress and write data in file
func writeSize(file *os.File, size uint32) error {
	// Write size of uncompressed data
	var sb [4]byte
	binary.LittleEndian.PutUint32(sb[:], size)
	if n, err := file.Write(sb[:]); uint32(n) != size || err != nil {
		return err
	}

	return nil
}

// writeCompressed compress and write data in file
func writeCompressed(file *os.File, data []byte) (n int, err error) {
	// Compress data
	var zb bytes.Buffer
	zw := brotli.NewWriter(&zb)
	_, err = zw.Write(data)
	_ = zw.Close()
	if err != nil {
		return
	}

	// Write size of uncompressed data
	if err = writeSize(file, uint32(len(data))); err != nil {
		return
	}

	// Write compressed data
	if n, err = file.Write(zb.Bytes()); n != zb.Len() || err != nil {
		return
	}

	return
}

func writeHeader(file *os.File, keys []string) (n int, err error) {
	// Write count of data chunks
	if err = writeSize(file, uint32(len(keys))); err != nil {
		return
	}

	// Write empty index table: compressed size of header & each date item
	index := make([]byte, 0, (len(keys)+1)*4)
	if n, err = file.Write(index); n != len(index) || err != nil {
		return
	}

	// Write join & compressed keys
	keyStr := strings.Join(keys, "#")
	if n, err = writeCompressed(file, []byte(keyStr)); err != nil {
		return
	}

	n += len(index) + len(keys)*4 + 4
	return
}

// save make physical saving data on disk
func (store *Sunduk) save(file *os.File) error {
	// Sort keys
	keys := make([]string, 0, len(store.index))
	for k := range store.index {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	if _, err := writeHeader(file, keys); err != nil {
		return err
	}

	// Pack data & recalc offsets
	sizes := make([]uint32, len(keys))
	for i, k := range keys {
		value, ok := store.data[k]
		if !ok {
			value, ok = store.Get(k)
			if !ok {
				return fmt.Errorf("storage consistancy is broken: value for key %q is not found", k)
			}
		}
		n, err := writeCompressed(file, value)
		if err != nil {
			return err
		}
		sizes[i] = uint32(n)
	}

	// Update header index
	if _, err := file.Seek(4, io.SeekStart); err != nil {
		return err
	}
	for _, cs := range sizes {
		var size [4]byte
		binary.LittleEndian.PutUint32(size[:], cs)
		if n, err := file.Write(size[:]); uint32(n) != cs || err != nil {
			return err
		}
	}
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(store.index)
	if err != nil {
		return err
	}

	var zbb bytes.Buffer
	gzw := brotli.NewWriter(&zbb)
	_, _ = gzw.Write(buf.Bytes())
	_ = gzw.Close()
	_ = store.file.Close()
	file, err := os.Create(store.FilePath)
	if err != nil {
		return err
	}
	var size [4]byte
	binary.BigEndian.PutUint32(size[:], uint32(zbb.Len()))
	_, _ = file.Write(size[:])

	binary.BigEndian.PutUint32(size[:], uint32(buf.Len()))
	_, _ = file.Write(size[:])
	_, _ = file.Write(zbb.Bytes())

	for _, k := range keys {
		s, err := file.Write(store.data[k])
		if s == 0 || err != nil {
			return err
		}
	}

	return nil
}
