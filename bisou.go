package main
import "fmt"
import "log"
import "sort"

type Node struct {
	name string
	args []*Node
}

func (p *Node) String() string {
    s := "(" + p.name
    for _, e := range p.args {
        s += " "
        if len(e.args) == 0 {
            s += e.name
        } else {
            s += e.String()
        }
    }
    s += ")"
    return s
}

func (p *Node) Ident() string {
    if p.name != "ident" || len(p.args) != 1 || len(p.args[0].args) != 0 { log.Fatal("assert") }
    return p.args[0].name
}

func Cons(name string, args ...*Node) *Node {
    return &Node{name:name, args:args}
}

// =======================

var p1 *Node = Cons("")
var p2 *Node = Cons("")
var p3 *Node = Cons("")

func (p *SQLParser) Push(name string, args ...*Node) *SQLParser {
    for i := range args {
        if args[i] == p1 {
            args[i] = p.Get(1)
        } else if args[i] == p2 {
            args[i] = p.Get(2)
        } else if args[i] == p3 {
            args[i] = p.Get(3)
        }
    }
    p.stack = append(p.stack[:len(p.stack)-p.size], &Node{name:name, args:args})
    p.size = 0
    //fmt.Println("stack: ", p.stack)
    return p
}

func (p *SQLParser) Push1(name string) *SQLParser {
    return p.Push(name, p.Get(1))
}

func (p *SQLParser) Push2(name string) *SQLParser {
    return p.Push(name, p.Get(2), p.Get(1))
}

func (p *SQLParser) Get(i int) *Node {
    if i > p.size { p.size = i }
    return p.stack[len(p.stack) - i]
}

func (p *SQLParser) Append() {
    a := append(p.Get(2).args, p.Get(1))
    p.Push(p.Get(2).name, a...)
}

// =======================

/*type StringColumn struct {
	data []string // string value or null
}

type NumericColumn struct {
	decimals int
	offset int64
	// number = (data[x] + offset) / 10^decimals

	data []uint64
	is_set []bool
}*/

type Column struct {
	name string
	data []string // one element per row
}

type Field struct {
	name string
	data string
}

type FieldsByName []Field
func (a FieldsByName) Len() int           { return len(a) }
func (a FieldsByName) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a FieldsByName) Less(i, j int) bool { return a[i].name < a[j].name }

func DataToString(data string) string {
	if len(data) == 0 { return "null" }
	if data[0] == '?' { return fmt.Sprintf("%q", data[1:]) }
	return data
}

type Row struct {
	fields []Field
}

type Table struct {
	columns []Column // sorted by name
	delta_rows []Row // temporary storage for row data until it becomes column data
}

var g_tables map[string]*Table

func PrintTable(table *Table) {
	for _, c := range table.columns {
		fmt.Printf("column:%s = [", c.name)
		for i, d := range c.data {
			if i > 0 { fmt.Print(" ") }
			fmt.Print(DataToString(d))
		}
		fmt.Println("]")
	}
}

func MergeDeltaRows(table *Table) {
	PrintTable(table)
	for i, row := range table.delta_rows {
		row_count := 0
		if len(table.columns) > 0 { row_count = len(table.columns[0].data) }

		a := 0
		b := 0
		for a < len(table.columns) && b < len(row.fields) {
			if table.columns[a].name < row.fields[b].name {
				table.columns[a].data = append(table.columns[a].data, "")
				a += 1
				continue
			}

			if table.columns[a].name > row.fields[b].name {
				table.columns = append(table.columns, Column{})
				for j := len(table.columns) - 1; j > a; j = j-1 { table.columns[j] = table.columns[j - 1] }
				table.columns[a] = Column{name:row.fields[b].name, data:make([]string, row_count, row_count + len(table.delta_rows) - i)}
			}
			table.columns[a].data = append(table.columns[a].data, row.fields[b].data)
			a += 1
			b += 1
		}
		for a < len(table.columns) {
			table.columns[a].data = append(table.columns[a].data, "")
			a += 1
		}
		for b < len(row.fields) {
			data := make([]string, row_count, row_count + len(table.delta_rows) - i)
			table.columns = append(table.columns, Column{name:row.fields[b].name, data:data})

			table.columns[a].data = append(table.columns[a].data, row.fields[b].data)
			a += 1
			b += 1
		}
		fmt.Println("after row")
		PrintTable(table)
	}

	table.delta_rows = table.delta_rows[0:0]
	for i := range table.columns { assert(len(table.columns[i].data) == len(table.columns[0].data)) }
}

func Insert(table string, insert *Node) {
	t, ok := g_tables[table]
	if !ok { log.Fatal(table) }
	if insert.name != "doc" { log.Fatal(insert.name) }

	row := Row{}
	row.fields = make([]Field, len(insert.args))
	for i, e := range insert.args {
		d := e.args[0]
		row.fields[i].name = e.name
		switch d.name {
		case "null": row.fields[i].data = ""
		case "str":  row.fields[i].data = "?" + d.args[0].name
		case "num":  row.fields[i].data = d.args[0].name; assert(len(d.args[0].name) > 0)
		default: log.Fatal("insert")
		}
	}
	sort.Sort(FieldsByName(row.fields))
	t.delta_rows = append(t.delta_rows, row)
}

type ResultSet struct {
	columns []string
	data [][]string
}

func Select(query *Node) ResultSet {
	assert(query.name == "select")
	args := query.args
	assert(len(args) == 2 && args[0].name == "project" && len(args[0].args) == 0 && args[1].name == "from" && len(args[1].args) == 1 && len(args[1].args[0].args) == 0)

	table, ok := g_tables[query.args[1].args[0].name]
	if !ok { log.Fatal("table not found") }
	MergeDeltaRows(table)

	var result ResultSet
	result.columns = make([]string, len(table.columns))
	for i := range table.columns { result.columns[i] = table.columns[i].name }

	rows := 0
	if len(table.columns) > 0 { rows = len(table.columns[0].data) }
	for i := 0; i < rows; i += 1 {
		row := make([]string, len(table.columns))
		for j := range table.columns { row[j] = table.columns[j].data[i] }
		result.data = append(result.data, row)
	}
	return result
}

func assert(cond bool) {
	if !cond { a := []int{}; a[3] = 0 }
}

func Parse(query string) *Node {
	parser := SQLParser{Buffer: query}
	parser.Init()
	err := parser.Parse()
	if err != nil { log.Fatal(err) }
    parser.Execute()
	if len(parser.stack) != 1 { log.Fatal(err) }
	return parser.stack[0]
}

func Execute(query *Node) {
	switch query.name {
	case "show_tables":
		for table, _ := range g_tables {
			fmt.Println(table)
		}
	case "create_table":
		assert(len(query.args) == 1 && len(query.args[0].args) == 0)
		table := query.args[0].name
	    if _, ok := g_tables[table]; !ok {
	        g_tables[table] = &Table{}
	    }
	case "drop_table":
		assert(len(query.args) == 1 && len(query.args[0].args) == 0)
		table := query.args[0].name
	    if _, ok := g_tables[table]; ok {
	        delete(g_tables, table)
	    }
	case "insert":
		assert(len(query.args) == 2 && len(query.args[0].args) == 0)
		Insert(query.args[0].name, query.args[1])
	case "select":
		result := Select(query)
		fmt.Printf("columns: %s\n", result.columns);
		for _, row := range result.data {
			for i, d := range row {
				if i > 0 { fmt.Print(" ") }
				fmt.Print(DataToString(d))
			}
			fmt.Println()
		}
	}
}

func Query(text string) {
	fmt.Println(text)
	ast := Parse(text)
	fmt.Println(ast)
	Execute(ast)
}

func main() {
	g_tables = make(map[string]*Table)
	Query("CREATE TABLE aa")
	/*Query("SHOW TABLES")
	Query("CREATE TABLE bb")
    Query("SHOW TABLES")
    Query("DROP TABLE bb")
    Query("SHOW TABLES")*/
	Query("INSERT INTO aa {id:10, name:'Marko', cost:-23.34, date:null, height:17}")
	Query("INSERT INTO aa {id:11, name:'Nikola', cost:12.3, date:'2013-05-03', alias:'Nick'}")
	Query("SELECT * FROM aa")
    /*Query("SELECT p FROM aa AS x, aa AS y")
	Query("SELECT aa FROM aa WHERE aa.id = 10")
	Query("DELETE FROM aa WHERE aa.id = 10")
	Query("SELECT a, 5 AS e FROM (SELECT b FROM c)")
    Query("SELECT DISTINCT x FROM a ORDER BY m, x DESC")*/
}
