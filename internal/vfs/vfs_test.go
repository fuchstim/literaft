package vfs_test

import (
	"errors"
	"path/filepath"
	"sync"
	"time"

	"github.com/ncruces/go-sqlite3"
	sqlite3vfs "github.com/ncruces/go-sqlite3/vfs"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("VFS.Open", func() {
	It("wraps a directly-opened temp file, and the wrapped file behaves like a plain file", func() {
		v := sqlite3vfs.Find(vfsName)

		f, _, err := v.Open("", sqlite3vfs.OPEN_TEMP_JOURNAL|sqlite3vfs.OPEN_CREATE|
			sqlite3vfs.OPEN_READWRITE|sqlite3vfs.OPEN_DELETEONCLOSE)
		Expect(err).NotTo(HaveOccurred())
		defer f.Close()

		data := []byte("hello from a directly-opened file")
		n, err := f.WriteAt(data, 0)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(len(data)))

		got := make([]byte, len(data))
		n, err = f.ReadAt(got, 0)
		Expect(err).NotTo(HaveOccurred())
		Expect(n).To(Equal(len(data)))
		Expect(got).To(Equal(data))
	})
})

var _ = Describe("wrapper VFS transparency", func() {
	It("produces logically identical results to the default VFS", func() {
		const ddl = `
			CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
			INSERT INTO t (v) VALUES ('a'), ('b'), ('c');
		`
		dir := GinkgoT().TempDir()

		wrapped := openDB(filepath.Join(dir, "wrapped.db"), vfsName)
		plain := openDB(filepath.Join(dir, "plain.db"), "")

		Expect(wrapped.Exec(ddl)).To(Succeed())
		Expect(plain.Exec(ddl)).To(Succeed())

		wantMode := queryText(plain, "PRAGMA journal_mode")
		Expect(wantMode).To(Equal("wal"))
		Expect(queryText(wrapped, "PRAGMA journal_mode")).To(Equal(wantMode))

		Expect(queryInt(wrapped, "SELECT count(*) FROM t")).To(Equal(queryInt(plain, "SELECT count(*) FROM t")))
		Expect(queryInt(wrapped, "PRAGMA page_count")).To(Equal(queryInt(plain, "PRAGMA page_count")))
		Expect(queryText(wrapped, "PRAGMA integrity_check")).To(Equal("ok"))
	})
})

var _ = Describe("multiple read-write connections", func() {
	It("keeps readers from observing an uncommitted write, and blocks a second writer", func() {
		path := filepath.Join(GinkgoT().TempDir(), "concurrent.db")

		writer := openDB(path, vfsName)

		Expect(writer.Exec("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)")).To(Succeed())
		Expect(writer.Exec("INSERT INTO t (v) VALUES (1)")).To(Succeed())

		reader := openDB(path, vfsName)

		Expect(writer.Exec("BEGIN IMMEDIATE")).To(Succeed())
		Expect(writer.Exec("INSERT INTO t (v) VALUES (2)")).To(Succeed())

		Expect(queryInt(reader, "SELECT count(*) FROM t")).To(Equal(int64(1)),
			"reader must not see the writer's uncommitted insert")

		second := openDB(path, vfsName)
		second.BusyTimeout(0)
		err := second.Exec("BEGIN IMMEDIATE")
		Expect(errors.Is(err, sqlite3.BUSY)).To(BeTrue(),
			"a second writer must be denied the write lock while the first txn is open, got: %v", err)
		second.Exec("ROLLBACK")

		Expect(writer.Exec("COMMIT")).To(Succeed())

		Expect(queryInt(reader, "SELECT count(*) FROM t")).To(Equal(int64(2)))
	})

	It("serializes concurrent writers on separate connections without lost updates", func() {
		path := filepath.Join(GinkgoT().TempDir(), "writers.db")

		setup := openDB(path, vfsName)

		Expect(setup.Exec("CREATE TABLE counter (id INTEGER PRIMARY KEY, v INTEGER)")).To(Succeed())
		Expect(setup.Exec("INSERT INTO counter (id, v) VALUES (1, 0)")).To(Succeed())

		const goroutines = 8
		const incrementsEach = 25

		var wg sync.WaitGroup
		for range goroutines {
			wg.Go(func() {
				defer GinkgoRecover()

				c := openDB(path, vfsName)
				c.BusyTimeout(5 * time.Second)
				for range incrementsEach {
					Expect(c.Exec("UPDATE counter SET v = v + 1 WHERE id = 1")).To(Succeed())
				}
			})
		}
		wg.Wait()

		verify := openDB(path, vfsName)
		Expect(queryInt(verify, "SELECT v FROM counter WHERE id = 1")).To(Equal(int64(goroutines * incrementsEach)))
	})
})
