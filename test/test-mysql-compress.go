package main

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"strconv"

	_ "github.com/hy-666/mysql"
)

const (
	ConnectionString = "huiyi:ou39ZwhtJwAhjvxjb@tcp(tencent.lizzy.tech:53306)/ds_cv_01?" +
		"allowAllFiles=true&allowNativePasswords=true&parseTime=true&loc=Local"
	ConnectionStringCompressed = ConnectionString + "&compress=true"
)

var (
	db           *sql.DB
	dbCompressed *sql.DB
)

func connect(compress bool) *sql.Conn {
	var err error
	var x *sql.DB
	if compress {
		if dbCompressed == nil {
			dbCompressed, err = sql.Open("mysql", ConnectionStringCompressed)
			if err != nil {
				panic(err)
			}
		}
		x = dbCompressed
	} else {
		if db == nil {
			db, err = sql.Open("mysql", ConnectionString)
			if err != nil {
				panic(err)
			}
		}
		x = db
	}
	b := context.Background()
	conn, err := x.Conn(b)
	if err != nil {
		panic(err)
	}
	return conn
}

func testSelect(compress bool, n int) {
	conn := connect(compress)
	defer conn.Close()
	query := "SELECT image_id,object_id,object_class_id,object_truncated,object_difficult,bbox_x1,bbox_y1,bbox_x2,bbox_y2,last_update_time FROM image_bbox_annotation"
	query += " ORDER BY image_id"
	query += " LIMIT " + strconv.FormatInt(int64(n), 10)
	rows, err := conn.QueryContext(context.Background(), query)
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var row struct {
			imageId        sql.NullString
			objectId       sql.NullInt32
			classId        sql.NullInt32
			truncated      sql.NullInt32
			difficult      sql.NullFloat64
			x1             sql.NullFloat64
			y1             sql.NullFloat64
			x2             sql.NullFloat64
			y2             sql.NullFloat64
			lastUpdateTime sql.NullTime
		}
		err = rows.Scan(&row.imageId, &row.objectId, &row.classId, &row.truncated, &row.difficult, &row.x1, &row.y1, &row.x2, &row.y2, &row.lastUpdateTime)
		if err != nil {
			panic(err)
		}
	}
	err = rows.Err()
	if err != nil {
		panic(err)
	}
}

func test1() {
	f, _ := os.Open("z://temp/packet.dat")
	s, _ := f.Stat()
	in := make([]byte, s.Size())
	f.Read(in)
	r1 := bytes.NewReader(in)
	var r2 bufio.Reader
	r2.Reset(r1)
	z, _ := zlib.NewReader(&r2)
	out := make([]byte, 16384)
	io.ReadFull(z, out)
	fmt.Println(len(out))
}

func main() {
	//test1()
	testSelect(true, 1000)
	fmt.Println("Hello World!")
}
