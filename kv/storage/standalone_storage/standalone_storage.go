package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"os"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db   *badger.DB
	path string
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	path := conf.DBPath
	options := badger.DefaultOptions
	options.Dir = path
	options.ValueDir = options.Dir
	return &StandAloneStorage{nil, path}

}

func (s *StandAloneStorage) Start() error {
	if err := os.MkdirAll(s.path, os.ModePerm); err != nil {
		log.Fatal(err)
	}

	options := badger.DefaultOptions

	options.Dir = s.path
	options.ValueDir = options.Dir

	db, err := badger.Open(options)
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return &Reader{db: s.db}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {

	batchUpdate := new(engine_util.WriteBatch)

	for i := 0; i < len(batch); i++ {
		data := batch[i]
		key := data.Key()
		cf := data.Cf()
		value := data.Value()
		if value != nil {
			batchUpdate.SetCF(cf, key, value)
		} else {
			batchUpdate.DeleteCF(cf, key)
		}
	}
	return batchUpdate.WriteToDB(s.db)
}

type Reader struct {
	db *badger.DB
}

func (r *Reader) GetCF(cf string, key []byte) ([]byte, error) {
	res, err := engine_util.GetCF(r.db, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return res, err
}
func (r *Reader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.db.NewTransaction(false))
}

func (r *Reader) Close() {
	// todo what need to do?
	r.db = nil
}
