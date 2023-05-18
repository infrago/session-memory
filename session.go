package session_memory

import (
	"encoding/base64"
	"errors"
	"sync"
	"time"

	"github.com/infrago/session"
	"github.com/tidwall/buntdb"
)

var (
	errInvalidCacheConnection = errors.New("Invalid session connection.")
	errEmptyData              = errors.New("Empty session data.")
)

type (
	memoryDriver struct {
	}
	memoryConnect struct {
		mutex sync.RWMutex

		instance *session.Instance
		setting  memorySetting

		db *buntdb.DB
	}
	memorySetting struct {
		Store string
	}
)

// 连接
func (driver *memoryDriver) Connect(inst *session.Instance) (session.Connect, error) {
	//获取配置信息
	setting := memorySetting{
		Store: ":memory:",
	}

	return &memoryConnect{
		instance: inst, setting: setting,
	}, nil
}

// 打开连接
func (this *memoryConnect) Open() error {
	if this.setting.Store == "" {
		return errors.New("无效会话存储")
	}
	db, err := buntdb.Open(this.setting.Store)
	if err != nil {
		return err
	}
	this.db = db
	return nil
}

// 关闭连接
func (this *memoryConnect) Close() error {
	if this.db != nil {
		if err := this.db.Close(); err != nil {
			return err
		}
	}
	return nil
}

// 查询会话，
func (this *memoryConnect) Read(id string) ([]byte, error) {
	if this.db == nil {
		return nil, errInvalidCacheConnection
	}

	value := ""
	err := this.db.View(func(tx *buntdb.Tx) error {
		vvv, err := tx.Get(id)
		if err != nil {
			return err
		}
		value = vvv
		return nil
	})
	if err == buntdb.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// 使用base64转换
	return base64.StdEncoding.DecodeString(value)
}

// 更新会话
func (this *memoryConnect) Write(id string, data []byte, expire time.Duration) error {
	if this.db == nil {
		return errInvalidCacheConnection
	}

	value := base64.StdEncoding.EncodeToString(data)
	if value == "" {
		return errEmptyData
	}

	return this.db.Update(func(tx *buntdb.Tx) error {
		opts := &buntdb.SetOptions{Expires: false}
		if expire > 0 {
			opts.Expires = true
			opts.TTL = expire
		}
		_, _, err := tx.Set(id, value, opts)
		return err
	})
}

// 查询会话，
func (this *memoryConnect) Exists(id string) (bool, error) {
	if this.db == nil {
		return false, errInvalidCacheConnection
	}

	err := this.db.View(func(tx *buntdb.Tx) error {
		_, err := tx.Get(id)
		return err
	})
	if err != nil {
		if err == buntdb.ErrNotFound {
			return true, nil
		}
	}
	return false, nil
}

// 删除会话
func (this *memoryConnect) Delete(id string) error {
	if this.db == nil {
		return errInvalidCacheConnection
	}

	return this.db.Update(func(tx *buntdb.Tx) error {
		_, err := tx.Delete(id)
		return err
	})
}

func (this *memoryConnect) Clear(prefix string) error {
	if this.db == nil {
		return errors.New("连接失败")
	}

	ids, err := this.Keys(prefix)
	if err != nil {
		return err
	}

	return this.db.Update(func(tx *buntdb.Tx) error {
		for _, id := range ids {
			_, err := tx.Delete(id)
			if err != nil {
				return err
			}
		}
		return nil
	})
}
func (this *memoryConnect) Keys(prefix string) ([]string, error) {
	if this.db == nil {
		return nil, errors.New("连接失败")
	}

	ids := []string{}
	err := this.db.View(func(tx *buntdb.Tx) error {
		tx.AscendKeys(prefix+"*", func(k, v string) bool {
			ids = append(ids, k)
			return true
		})

		return nil
	})
	if err != nil {
		return nil, err
	}

	return ids, nil
}
