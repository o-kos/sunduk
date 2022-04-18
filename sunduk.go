package sunduk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/andybalholm/brotli"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
)

type entry struct {
	Offset int64
	Size   int32
}

type Sunduk struct {
	FilePath string // FilePath is the path to the file used to persist

	file  *os.File
	data  map[string][]byte
	index map[string]entry
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
	_, err := store.file.Seek(entry.Offset, 0)
	if err != nil {
		return
	}

	data := make([]byte, entry.Size)
	data = data[:cap(data)]
	n, err := store.file.Read(data)
	if int32(n) != entry.Size || err != nil {
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
	store.index[key] = entry{0, int32(len(value))}
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
	store.index = make(map[string]entry)
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
	// File exist, so we need to read it
	store.file = file
	return store.readHeader()
}

// readHeader read, decompress and unmarshall storage header
func (store *Sunduk) readHeader() error {
	makeErr := func(action string, err error) error {
		return fmt.Errorf("unable to %s storage header: %v", action, err)
	}

	// Read count of keys in storage
	var sb [4]byte
	if n, err := store.file.Read(sb[:]); n != len(sb) || err != nil {
		return makeErr("read count of keys in", err)
	}
	kc := binary.LittleEndian.Uint32(sb[:])

	// Read compressed size of keys
	if n, err := store.file.Read(sb[:]); n != len(sb) || err != nil {
		return makeErr("read size of keys chunk in", err)
	}
	ks := binary.LittleEndian.Uint32(sb[:])

	// Read compressed sizes of data chunks
	sizes := make([]uint32, kc)
	for i := uint32(0); i < kc; i++ {
		if n, err := store.file.Read(sb[:]); n != len(sb) || err != nil {
			return makeErr("read size of data chunk in", err)
		}
		sizes[i] = binary.LittleEndian.Uint32(sb[:])
	}

	// Read compressed header content
	data := make([]byte, ks)
	if n, err := store.file.Read(data[:]); uint32(n) != ks || err != nil {
		return makeErr("read", err)
	}

	// Save offset of storage data
	offset, err := store.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return makeErr("seek position in", err)
	}

	// Decompress header
	zr := brotli.NewReader(bytes.NewReader(data))
	header, err := ioutil.ReadAll(zr)
	if err != nil {
		return makeErr("decompress", err)
	}

	// Unmarshall header data
	keys := strings.Split(string(header), "#")
	if uint32(len(keys)) != kc {
		return makeErr("decode keys in", err)
	}
	for i, k := range keys {
		store.index[k] = entry{offset, int32(sizes[i])}
		offset += int64(sizes[i])
	}
	//dec := gob.NewDecoder(bytes.NewBuffer(header))
	//if err := dec.Decode(&store.index); err != nil {
	//	return makeErr("decode", err)
	//}

	return nil
}

// flush combines all entries recorded in the file and re-saves only the necessary entries.
// The function is executed on creation, but can also be executed manually if storage space is a concern.
// The original file is backed up
func (store *Sunduk) flush() error {
	// Create new file for saving data
	newname := store.FilePath + ".new"
	file, err := os.Create(newname)
	if err != nil {
		return err
	}
	defer func(file *os.File) {
		_ = file.Close()
		_ = os.Remove(newname)
	}(file)

	// Save storage contents on disk
	if err := store.save(file); err != nil {
		return fmt.Errorf("unable to create %s file for flushing: %s", newname, err.Error())
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("unable to close %s file after flushing: %s", newname, err.Error())
	}

	// Back up the old file before doing the flushing
	store.Close()
	bakname := store.FilePath + ".bak"
	if err := os.Rename(store.FilePath, bakname); err != nil {
		return fmt.Errorf("unable to rename %s to %s during flushing: %s", store.FilePath, bakname, err.Error())
	}
	defer func(file *os.File) {
		_ = os.Remove(bakname)
	}(file)

	if err := os.Rename(newname, store.FilePath); err != nil {
		return fmt.Errorf("unable to save new file at %s during flushing: %s", store.FilePath, err.Error())
	}

	return nil
}

// writeSize compress and write data in file
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

	//// Write size of uncompressed data
	//if err = writeSize(file, uint32(len(data))); err != nil {
	//	return
	//}

	// Write compressed data
	if n, err = file.Write(zb.Bytes()); n != zb.Len() || err != nil {
		return
	}

	return
}

// writeHeader format header space for save data
// Header format is
// uint32 Count						- count of data chunks
// uint32 Size of keys chunk		- compressed size of keys chunk
// uint32 Size of first data chunk  - compressed size of data chunk
// uint32 Size of next data chunk
// ...
// uint32 Size of last  data chunk  - compressed size of data chunk
//
func writeHeader(file *os.File, keys []string) (n int, err error) {
	// Write count of data chunks
	if err = writeSize(file, uint32(len(keys))); err != nil {
		return
	}

	// Write empty index table: compressed size of header & each date item
	index := make([]byte, (len(keys)+1)*4)
	if n, err = file.Write(index); n != len(index) || err != nil {
		return
	}

	// Write join & compressed keys
	keyStr := strings.Join(keys, "#")
	if n, err = writeCompressed(file, []byte(keyStr)); err != nil {
		return
	}
	ks := uint32(n)

	// Write compressed size of header in begin of index table
	if _, err = file.Seek(4, io.SeekStart); err != nil {
		return
	}
	if err = writeSize(file, ks); err != nil {
		return
	}
	if _, err = file.Seek(0, io.SeekEnd); err != nil {
		return
	}

	n += len(index) + 4
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
	if _, err := file.Seek(2*4, io.SeekStart); err != nil {
		return err
	}
	for _, cs := range sizes {
		if err := writeSize(file, cs); err != nil {
			return err
		}
	}

	return nil
}
