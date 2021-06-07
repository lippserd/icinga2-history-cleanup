package contracts

var Tables = []Table{
	{"statehistory", "statehistory_id", "state_time"},
}

type Table struct {
	Name       string
	PrimaryKey string
	Column     string
}
