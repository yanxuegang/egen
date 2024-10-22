package code

import (
	"context"
	"database/sql"
	"errors"
	"strings"
)

type UserGenDAO struct {
	session interface {
		QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
		QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
		ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	}
}

type UserTxGenDAO struct {
	*UserGenDAO
}

func (dao *UserTxGenDAO) Rollback() error {
	tx, ok := dao.session.(*sql.Tx)
	if !ok {
		return errors.New("非事务")
	}
	return tx.Rollback()
}

func (dao *UserTxGenDAO) Commit() error {
	tx, ok := dao.session.(*sql.Tx)
	if !ok {
		return errors.New("非事务")
	}
	return tx.Commit()
}

func (dao *UserGenDAO) Begin(ctx context.Context, opts *sql.TxOptions) (*UserTxGenDAO, error) {
	db, ok := dao.session.(*sql.DB)
	if !ok {
		return nil, errors.New("不能在事务中开启事务")
	}
	tx, err := db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &UserTxGenDAO{
		UserGenDAO: &UserGenDAO{tx},
	}, nil
}

func NewUserGenDAO(db *sql.DB) (*UserGenDAO, error) {
	return &UserGenDAO{db}, nil
}

func (dao *UserGenDAO) Insert(ctx context.Context, vals ...*User) (int64, error) {
	if len(vals) == 0 || vals == nil {
		return 0, nil
	}
	var args = make([]interface{}, 0, len(vals)*(5))
	var str = ""
	for k, v := range vals {
		if k != 0 {
			str += ", "
		}
		str += "(?,?,?,?,?)"
		args = append(args, v.LoginTime, v.FirstName, v.LastName, v.UserId, v.Password)
	}
	sqlSen := "INSERT INTO `user`(`login_time`,`first_name`,`last_name`,`user_id`,`password`) VALUES" + str
	res, err := dao.session.ExecContext(ctx, sqlSen, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (dao *UserGenDAO) NewOne(row *sql.Row) (*User, error) {
	if err := row.Err(); err != nil {
		return nil, err
	}
	var val User
	err := row.Scan(&val.LoginTime, &val.FirstName, &val.LastName, &val.UserId, &val.Password)
	return &val, err
}

func (dao *UserGenDAO) SelectByRaw(ctx context.Context, query string, args ...any) (*User, error) {
	row := dao.session.QueryRowContext(ctx, query, args...)
	return dao.NewOne(row)
}

func (dao *UserGenDAO) SelectByWhere(ctx context.Context, where string, args ...any) (*User, error) {
	s := "SELECT `login_time`,`first_name`,`last_name`,`user_id`,`password` FROM `user` WHERE " + where
	return dao.SelectByRaw(ctx, s, args...)
}

func (dao *UserGenDAO) NewBatch(rows *sql.Rows) ([]*User, error) {
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var vals = make([]*User, 0, 5)
	for rows.Next() {
		var val User
		if err := rows.Scan(&val.LoginTime, &val.FirstName, &val.LastName, &val.UserId, &val.Password); err != nil {
			return nil, err
		}
		vals = append(vals, &val)
	}
	return vals, nil
}

func (dao *UserGenDAO) SelectBatchByRaw(ctx context.Context, query string, args ...any) ([]*User, error) {
	rows, err := dao.session.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return dao.NewBatch(rows)
}

func (dao *UserGenDAO) SelectBatchByWhere(ctx context.Context, where string, args ...any) ([]*User, error) {
	s := "SELECT `login_time`,`first_name`,`last_name`,`user_id`,`password` FROM `user` WHERE " + where
	return dao.SelectBatchByRaw(ctx, s, args...)
}

func (dao *UserGenDAO) UpdateSpecificColsByWhere(ctx context.Context, val *User, cols []string, where string, args ...any) (int64, error) {
	newArgs, colAfter := dao.quotedSpecificCol(val, cols...)
	newArgs = append(newArgs, args...)
	s := "UPDATE `user` SET " + colAfter + " WHERE " + where
	return dao.UpdateColsByRaw(ctx, s, newArgs...)
}

func (dao *UserGenDAO) UpdateNoneZeroColByWhere(ctx context.Context, val *User, where string, args ...any) (int64, error) {
	newArgs, colAfter := dao.quotedNoneZero(val)
	newArgs = append(newArgs, args...)
	s := "UPDATE `user` SET " + colAfter + " WHERE " + where
	return dao.UpdateColsByRaw(ctx, s, newArgs...)
}

func (dao *UserGenDAO) UpdateNonePKColByWhere(ctx context.Context, val *User, where string, args ...any) (int64, error) {
	newArgs, colAfter := dao.quotedNonePK(val)
	newArgs = append(newArgs, args...)
	s := "UPDATE `user` SET " + colAfter + " WHERE " + where
	return dao.UpdateColsByRaw(ctx, s, newArgs...)
}

func (dao *UserGenDAO) quotedNoneZero(val *User) ([]interface{}, string) {
	var cols = make([]string, 0, 5)
	var args = make([]interface{}, 0, 5)
	if val.LoginTime != "" {
		args = append(args, val.LoginTime)
		cols = append(cols, "`login_time`")
	}
	if val.FirstName != "" {
		args = append(args, val.FirstName)
		cols = append(cols, "`first_name`")
	}
	if val.LastName != "" {
		args = append(args, val.LastName)
		cols = append(cols, "`last_name`")
	}
	if val.UserId != 0 {
		args = append(args, val.UserId)
		cols = append(cols, "`user_id`")
	}
	if val.Password != nil {
		args = append(args, val.Password)
		cols = append(cols, "`password`")
	}
	if len(cols) == 1 {
		cols[0] = cols[0] + "=?"
	} else {
		cols[len(cols)-1] = cols[len(cols)-1] + "=?"
	}
	return args, strings.Join(cols, "=?,")
}

func (dao *UserGenDAO) quotedNonePK(val *User) ([]interface{}, string) {
	var cols = []string{"`login_time`", "`first_name`", "`last_name`", "`password`"}
	var args = []interface{}{val.LoginTime, val.FirstName, val.LastName, val.Password}
	if len(cols) == 1 {
		cols[0] = cols[0] + "=?"
	} else {
		cols[len(cols)-1] = cols[len(cols)-1] + "=?"
	}
	return args, strings.Join(cols, "=?,")
}

func (dao *UserGenDAO) quotedSpecificCol(val *User, cols ...string) ([]interface{}, string) {
	var relation = make(map[string]interface{}, 5)
	var args = make([]interface{}, 0, 5)
	relation["first_name"] = val.FirstName
	relation["last_name"] = val.LastName
	relation["login_time"] = val.LoginTime
	relation["password"] = val.Password
	relation["user_id"] = val.UserId
	for i := 0; i < len(cols); i++ {
		args = append(args, relation[cols[i]])
		cols[i] = "`" + cols[i] + "`"
	}
	if len(cols) == 1 {
		cols[0] = cols[0] + "=?"
	} else {
		cols[len(cols)-1] = cols[len(cols)-1] + "=?"
	}
	return args, strings.Join(cols, "=?,")
}

func (dao *UserGenDAO) UpdateColsByRaw(ctx context.Context, query string, args ...any) (int64, error) {
	res, err := dao.session.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (dao *UserGenDAO) DeleteByWhere(ctx context.Context, where string, args ...any) (int64, error) {
	s := "DELETE FROM `user` WHERE " + where
	return dao.DeleteByRaw(ctx, s, args...)
}

func (dao *UserGenDAO) DeleteByRaw(ctx context.Context, query string, args ...any) (int64, error) {
	res, err := dao.session.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
