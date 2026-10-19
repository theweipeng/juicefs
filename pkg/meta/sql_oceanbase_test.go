//go:build !nomysql && !nosqlite

/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package meta

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"
	"xorm.io/xorm"
	"xorm.io/xorm/core"
	"xorm.io/xorm/dialects"
)

func TestOceanBaseSerializationRetry(t *testing.T) {
	conflict := &mysql.MySQLError{
		Number:   6235,
		SQLState: [5]byte{'2', '5', '0', '0', '0'},
		Message:  "can't serialize access for this transaction",
	}
	for _, tc := range []struct {
		name  string
		err   error
		retry bool
	}{
		{"serialization_conflict", conflict, true},
		{"wrapped_serialization_conflict", fmt.Errorf("delete directory: %w", conflict), true},
		{"permission_denied", &mysql.MySQLError{Number: 1142, Message: "DELETE command denied"}, false},
		{"other_oceanbase_error", &mysql.MySQLError{Number: 6236, Message: "transaction weak read version is not ready"}, false},
		{"io_error", syscall.EIO, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newOceanBaseRetryTestMeta(t)
			require.Equal(t, tc.retry, m.shouldRetry(tc.err))
		})
	}
}

func TestOceanBaseTransactionRollbackAndRetry(t *testing.T) {
	for _, retry := range []bool{true, false} {
		t.Run(fmt.Sprintf("retry=%t", retry), func(t *testing.T) {
			m := newOceanBaseRetryTestMeta(t)
			_, err := m.db.Exec("CREATE TABLE retry_probe (value INTEGER NOT NULL)")
			require.NoError(t, err)
			_, err = m.db.Exec("INSERT INTO retry_probe VALUES (0)")
			require.NoError(t, err)

			var injected error = syscall.EIO
			if retry {
				injected = &mysql.MySQLError{
					Number:   6235,
					SQLState: [5]byte{'2', '5', '0', '0', '0'},
					Message:  "can't serialize access for this transaction",
				}
			}
			attempts := 0
			err = m.txn(func(s *xorm.Session) error {
				attempts++
				rows, queryErr := s.QueryString("SELECT value FROM retry_probe")
				require.NoError(t, queryErr)
				// 重试必须从已回滚的状态开始，不能重复提交上次的部分写入。
				require.Equal(t, "0", rows[0]["value"])
				_, updateErr := s.Exec("UPDATE retry_probe SET value = value + 1")
				require.NoError(t, updateErr)
				if attempts == 1 {
					return fmt.Errorf("delete directory: %w", injected)
				}
				return nil
			})
			rows, queryErr := m.db.QueryString("SELECT value FROM retry_probe")
			require.NoError(t, queryErr)
			if retry {
				require.NoError(t, err)
				require.Equal(t, 2, attempts)
				require.Equal(t, "1", rows[0]["value"])
			} else {
				require.True(t, errors.Is(err, injected))
				require.Equal(t, 1, attempts)
				require.Equal(t, "0", rows[0]["value"])
			}
		})
	}
}

func newOceanBaseRetryTestMeta(t *testing.T) *dbMeta {
	t.Helper()
	// 用 SQLite 执行真实事务，MySQL 名称仅选择重试分支；冲突由测试注入。
	dsn := filepath.Join(t.TempDir(), "retry.db")
	db, err := sql.Open("sqlite3", dsn)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	dialect, err := dialects.OpenDialect("sqlite3", dsn)
	require.NoError(t, err)
	engine, err := xorm.NewEngineWithDialectAndDB("mysql", dsn, dialect, core.FromDB(db))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, engine.Close()) })
	return &dbMeta{baseMeta: newBaseMeta(dsn, testConfig()), db: engine}
}
