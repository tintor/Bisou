package main
import "fmt"
import "log"

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

type Segment struct {
	
}

type Table struct {
	docs []*Node
}

var g_tables map[string]*Table

func ShowTables() {
    for name, table := range g_tables {
        fmt.Printf("%s %d\n", name, table.docs)
    }
}

func CreateTable(table string) {
    if _, ok := g_tables[table]; !ok {
        g_tables[table] = &Table{}
    }
}

func DropTable(table string) {
    if _, ok := g_tables[table]; ok {
        delete(g_tables, table)
    }
}

func Insert(table string, insert *Node) {
    fmt.Printf("insert %s\n", insert)
}

func Select(args []*Node) {
    fmt.Printf("select %s\n", args)
}

func Delete(args []*Node) {
    fmt.Printf("delete %s\n", args)
}

func Execute(query string) {
	fmt.Println(query)
	parser := &SQLParser{Buffer: query}
	parser.Init()
	err := parser.Parse()
	if err != nil {
		log.Fatal(err)
	}
    parser.Execute()
}

func main() {
	g_tables = make(map[string]*Table)
	Execute("CREATE TABLE aa")
	/*Execute("SHOW TABLES")
	Execute("CREATE TABLE bb")
    Execute("SHOW TABLES")
    Execute("DROP TABLE bb")
    Execute("SHOW TABLES")*/
	Execute("INSERT INTO aa {id:10, name:'Marko', cost:-23.34, date:null}")
	Execute("INSERT INTO aa {id:11, name:'Nikola', cost:12.3, date:'2013-05-03'}")
	Execute("SELECT 1+2*3 OR 1*2+3 FROM aa")
    Execute("SELECT p FROM aa AS x, aa AS y")
	Execute("SELECT aa FROM aa WHERE aa.id = 10")
	Execute("DELETE FROM aa WHERE aa.id = 10")
	Execute("SELECT a, 5 AS e FROM (SELECT b FROM c)")
    Execute("SELECT DISTINCT x FROM a ORDER BY m, x DESC")
}
