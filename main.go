package main

import (
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gocraft/dbr"
)

func main() {
	if err := _main(); err != nil {
		panic(err)
	}
}

func _main() error {
	conn, err := dbr.Open("mysql", "root:@tcp(mysql:3306)/test", nil)
	if err != nil {
		return err
	}

	for {
		if err := conn.Ping(); err != nil {
			fmt.Println(err)
			time.Sleep(time.Second)
			continue
		}
		break
	}

	if err := readyTables(conn); err != nil {
		return err
	}
	if err := create(conn); err != nil {
		return err
	}
	if err := readList(conn); err != nil {
		return err
	}
	if err := union(conn); err != nil {
		return err
	}

	return nil
}

func readyTables(conn *dbr.Connection) error {
	for _, q := range []string{
		`
			create table users (
				id int not null auto_increment,
				name varchar(255),
				gender varchar(255),
				age int,
				primary key (id)
			)
		`,
		`
			create table posts (
				id int not null auto_increment,
				user_id int not null,
				body varchar(1000),
				primary key (id),
				index idx_user_id (user_id),
				foreign key (user_id) references users (id)
			)
		`,
	} {
		_, err := conn.Exec(q)
		if err != nil {
			return err
		}
	}
	return nil
}

type User struct {
	ID     dbr.NullInt64
	Name   dbr.NullString
	Gender dbr.NullString
	Age    dbr.NullInt64
}

type Post struct {
	ID     dbr.NullInt64
	Body   dbr.NullString
	UserID dbr.NullInt64 `db:"user_id"`
	User   User          `db:"-"`
}

func create(conn *dbr.Connection) error {
	sess := conn.NewSession(nil)

	for _, u := range []User{
		{
			Name:   dbr.NewNullString("Foo"),
			Gender: dbr.NewNullString("male"),
			Age:    dbr.NewNullInt64(29),
		},
		{
			Name:   dbr.NewNullString("Bar"),
			Gender: dbr.NewNullString("female"),
			Age:    dbr.NewNullInt64(17),
		},
		{
			Name:   dbr.NewNullString("Baz"),
			Gender: dbr.NewNullString("male"),
			Age:    dbr.NewNullInt64(41),
		},
		{
			Name:   dbr.NewNullString("Qux"),
			Gender: dbr.NewNullString("female"),
			Age:    dbr.NewNullInt64(32),
		},
		{
			Name:   dbr.NewNullString("Hoge"),
			Gender: dbr.NewNullString("male"),
			Age:    dbr.NewNullInt64(11),
		},
		{
			Name:   dbr.NewNullString("Fuga"),
			Gender: dbr.NewNullString("female"),
			Age:    dbr.NewNullInt64(51),
		},
	} {
		_, err := sess.InsertInto("users").Columns("name", "gender", "age").Record(&u).Exec()
		if err != nil {
			return err
		}
	}

	for _, u := range []Post{
		{
			UserID: dbr.NewNullInt64(1),
			Body:   dbr.NewNullString("AAAAAAAAAAA"),
		},
		{
			UserID: dbr.NewNullInt64(1),
			Body:   dbr.NewNullString("BBBBBBBBBBBBB"),
		},
		{
			UserID: dbr.NewNullInt64(2),
			Body:   dbr.NewNullString("CCCCCC"),
		},
	} {
		_, err := sess.InsertInto("posts").Columns("user_id", "body").Record(&u).Exec()
		if err != nil {
			return err
		}
	}

	return nil
}

func readList(conn *dbr.Connection) error {
	sess := conn.NewSession(nil)

	var (
		ps []Post
		us []User
	)
	if _, err := sess.
		Select("*").
		From("posts").
		Load(&ps); err != nil {
		return err
	}

	uids := make([]dbr.NullInt64, len(ps))
	for i, p := range ps {
		uids[i] = p.UserID
	}
	fmt.Println("uids = ", uniq(uids))
	if _, err := sess.
		Select("*").
		From("users").
		Where("id IN ?", uniq(uids)).
		Load(&us); err != nil {
		return err
	}
	users, err := json.MarshalIndent(us, "", "  ")
	if err != nil {
		return err
	}
	fmt.Printf("posts = %v\n", string(users))

	umap := map[int64]User{}
	for _, u := range us {
		if u.ID.Valid {
			umap[u.ID.Int64] = u
		}
	}
	for i, p := range ps {
		if p.ID.Valid {
			p.User = umap[p.UserID.Int64]
			ps[i] = p
		}
	}

	posts, err := json.MarshalIndent(ps, "", "  ")
	if err != nil {
		return err
	}
	fmt.Printf("posts = %v\n", string(posts))

	return err
}

func uniq(ns []dbr.NullInt64) []int64 {
	m := map[int64]bool{}
	for _, n := range ns {
		if n.Valid {
			m[n.Int64] = true
		}
	}
	var s []int64
	for i, _ := range m {
		s = append(s, i)
	}
	return s
}

func union(conn *dbr.Connection) error {
	var us []User
	sess := conn.NewSession(nil)
	if _, err := sess.Select("*").From(
		dbr.Union(
			dbr.Select("*").From("users").Where(
				dbr.And(
					dbr.Gt("age", 20),
					dbr.Eq("gender", "male"),
				),
			),
			dbr.Select("*").From("users").Where(
				dbr.And(
					dbr.Lt("age", 50),
					dbr.Eq("gender", "female"),
				),
			),
		).As("uni"),
	).Load(&us); err != nil {
		return err
	}

	users, err := json.MarshalIndent(us, "", "  ")
	if err != nil {
		return err
	}
	fmt.Printf("union users = %v\n", string(users))

	return nil
}
