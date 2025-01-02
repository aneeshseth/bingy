package db

import (
	"binge/db/migr"
	"context"
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type DB struct {
	db   *sql.DB
	migr *migr.Queries
}

func NewDB(sqlUser string, sqlPass string, globalDB string) (*DB, error) {
	dbConnURL := fmt.Sprintf("%s:%s@tcp(localhost:3306)/%s?parseTime=true", sqlUser, sqlPass, globalDB)
	db, err := sql.Open("mysql", dbConnURL)
	if err != nil {
		return nil, err
	}

	queries := migr.New(db)

	dbType := &DB{
		db:   db,
		migr: queries,
	}

	return dbType, nil
}

func (d *DB) InsertUser(ctx context.Context, params migr.InsertUserParams) error {
	return d.migr.InsertUser(ctx, params)
}

func (d *DB) InsertSwipe(ctx context.Context, params migr.InsertSwipeParams) error {
	return d.migr.InsertSwipe(ctx, params)
}

func (d *DB) InsertMatch(ctx context.Context, params migr.InsertMatchParams) error {
	return d.migr.InsertMatch(ctx, params)
}
