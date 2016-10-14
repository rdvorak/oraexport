package main

import (
	//"fmt"
	"encoding/csv"
	"log"
	"os"
	"strings"

	"github.com/urfave/cli"
	"gopkg.in/rana/ora.v3"
	//	"unicode/utf8"
	"bufio"
	"strconv"
)

func row2string(x []interface{}) []string {
	var s []string
	for _, f := range x {
		switch t := f.(type) {
		case int64:
			s = append(s, strconv.FormatInt(f.(int64), 10))
		case ora.Int64:
			if f.(ora.Int64).IsNull {
				s = append(s, nullText)
			} else {
				s = append(s, strconv.FormatInt(f.(ora.Int64).Value, 10))
			}
		case ora.Float64:
			if f.(ora.Float64).IsNull {
				s = append(s, nullText)
			} else {
				s = append(s, strconv.FormatFloat(f.(ora.Float64).Value, 'f', -1, 64))
			}
		case ora.OCINum:
			s = append(s, f.(ora.OCINum).String())
		case float64:
			s = append(s, strconv.FormatFloat(f.(float64), 'f', -1, 64))
		case string:
			s = append(s, f.(string))
		case ora.Time:
			if f.(ora.Time).IsNull {
				s = append(s, nullText)
			} else {
				s = append(s, f.(ora.Time).Value.Format(dateFormat))
			}
		default:
			s = append(s, f.(string))
			_ = t
		}
	}
	return s
}

var user, password, dblink, dateFormat, nullText string
var comma string
var header, useCRLF bool
var sql, refCursor cli.StringSlice

func main() {

	app := cli.NewApp()
	app.Flags = []cli.Flag{
		cli.StringFlag{Name: "user, u", Destination: &user, EnvVar: "ORADB_USER"},
		cli.StringFlag{Name: "password, p", Destination: &password, EnvVar: "ORADB_PASSWORD"},
		cli.StringFlag{Name: "connect, c", Destination: &dblink, EnvVar: "ORADB_CONNECT", Usage: "example: \"localhost:1524/MIS.OK.AERO\""},
		cli.BoolFlag{Name: "withHeader", Destination: &header, Usage: "The first line are column names of the query/cursor"},
		cli.BoolFlag{Name: "useCRLF", Destination: &useCRLF, Usage: "Lines are delimited by CRLF instead of LF"},
		cli.StringFlag{Name: "delimiter, d", Value: ",", Destination: &comma, Usage: "example: \"\\t\" for tab "},
		cli.StringFlag{Name: "nullText", Value: "", Destination: &nullText, Usage: "The value is textual representation of oracle null values"},
		cli.StringSliceFlag{Name: "query, q", Value: &sql, Usage: "The query is read from standard input if it is not passed by query parameter, which can be repeated: -q \"select sysdate\" -q \"from dual\""},
		cli.StringSliceFlag{Name: "refCursor", Value: &refCursor, Usage: "PLSQL output parameter must be SYS_REFCURSOR: \"begin :1 := func_returning_sys_refcursor(); end;\" | \"CALL proc_returning_sys_refcursor(:1)\""},
		cli.StringFlag{Name: "dateFormat", Value: "2006-01-02T15:04:05", Destination: &dateFormat, Usage: "format is the desired textual representation of the reference time: Mon Jan 2 15:04:05 -0700 MST 2006"},
	}
	app.Action = func(c *cli.Context) error {
		env, err := ora.OpenEnv(nil)
		defer env.Close()
		if err != nil {
			panic(err)
		}
		rsetcfg := ora.NewRsetCfg()
		// stmt bude vrace nullable types s atributem Isnull - jinak nam napr null Integer vraci 0
		rsetcfg.SetNumberBigInt(ora.OraI64)
		rsetcfg.SetNumberInt(ora.OraI64)
		rsetcfg.SetNumberFloat(ora.OraF64)
		rsetcfg.SetFloat(ora.OraF64)
		rsetcfg.SetDate(ora.OraT)
		rsetcfg.SetTimestamp(ora.OraT)
		rsetcfg.SetTimestampTz(ora.OraT)
		rsetcfg.SetTimestampLtz(ora.OraT)
		/*
			srvcfg.StmtCfg.Rset.SetChar1(ora.OraS)
			srvcfg.StmtCfg.Rset.SetVarchar(ora.OraS)
			srvcfg.StmtCfg.Rset.SetLong(ora.OraS)
			srvcfg.StmtCfg.Rset.SetClob(ora.OraS)
			srvcfg.StmtCfg.Rset.SetBlob(ora.OraBin)
			srvcfg.StmtCfg.Rset.SetRaw(ora.OraBin)
			srvcfg.StmtCfg.Rset.SetLongRaw(ora.OraBin)
		*/
		stmtcfg := ora.NewStmtCfg()
		stmtcfg.Rset = rsetcfg
		srvcfg := &ora.SrvCfg{Dblink: dblink, StmtCfg: stmtcfg}
		srv, err := env.OpenSrv(srvcfg)
		defer srv.Close()
		if err != nil {
			panic(err)
		}
		sesCfg := ora.SesCfg{
			Username: user,
			Password: password,
		}
		ses, err := srv.OpenSes(&sesCfg)
		defer ses.Close()
		if err != nil {
			panic(err)
		}
		if len(sql) == 0 && len(refCursor) == 0 {
			// pokud nemame select ani cursor z prikazove radky, ocekavame select z Stdin
			scanner := bufio.NewScanner(os.Stdin)
			for scanner.Scan() {
				sql = append(sql, scanner.Text())
			}
			if err = scanner.Err(); err != nil {
				log.Fatalln("Error reading standard input:", err)
			}
		}
		if len(sql) > 0 && len(refCursor) > 0 {
			// nemuzeme mit jak select tak cursor
			log.Fatalln("The input cannot be both SQL and cursor")
		}
		if len(sql) == 0 && len(refCursor) == 0 {
			// chybi select nebo cursor na vstupu
			log.Fatalln("There is no input SQL or cursor")
		}
		w := csv.NewWriter(os.Stdout)
		if comma != "" {
			// tabulator jako cmdline parameter -d "\t" ( dvojite uvozovky!)
			s, _, _, _ := strconv.UnquoteChar(comma, '"')
			w.Comma = s
		}
		if useCRLF {
			w.UseCRLF = true
		}
		if len(sql) > 0 {
			sqlstmt := strings.Join(sql, " ")
			stmtQry, err := ses.Prep(sqlstmt)
			rset, err := stmtQry.Qry()
			defer stmtQry.Close()
			if err != nil {
				panic(err)
			}
			if err := w.Write(rset.ColumnNames); err != nil {
				log.Fatalln("error writing record to csv:", err)
			}
			for rset.Next() {
				if err := w.Write(row2string(rset.Row)); err != nil {
					log.Fatalln("error writing record to csv:", err)
				}
			}
			if rset.Err != nil {
				panic(rset.Err)
			}
		} else if len(refCursor) > 0 {
			// priklady vstupu pro kurzor
			// refCursor "begin :1 := funkce_vracejici_sys_refcursor(); end;"
			// refCursor "call procedura_vracejici_sys_refcursor(:1) "
			sqlstmt := strings.Join(refCursor, " ")
			stmtQry, err := ses.Prep(sqlstmt)
			defer stmtQry.Close()
			if err != nil {
				panic(err)
			}
			var rset ora.Rset
			_, err = stmtQry.Exe(&rset)
			if err != nil {
				panic(err)
			}
			if rset.IsOpen() {
				if err := w.Write(rset.ColumnNames); err != nil {
					log.Fatalln("error writing record to csv:", err)
				}
				for rset.Next() {
					if err := w.Write(row2string(rset.Row)); err != nil {
						log.Fatalln("error writing record to csv:", err)
					}
				}
			}

			if rset.Err != nil {
				panic(rset.Err)
			}
		}

		w.Flush()
		if err := w.Error(); err != nil {
			log.Fatal(err)
		}
		return nil
	}
	app.Run(os.Args)
}
