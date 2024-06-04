package setup

// TestKeyspace is the dedicated keyspace for testing
const TestKeyspace = "zdmproxy_test"

// TasksModel is the dedicated table for testing
const TasksModel = "tasks"

// PeopleModel is another dedicated table for testing
const PeopleModel = "people"

// EmployeeModel is dedicated table for SAI index testing
const EmployeeModel = "employee"

var DataModels = map[string]DataModel{
	TasksModel: SimpleDataModel{
		table: TasksModel,
	},
	PeopleModel: SimpleDataModel{
		table: PeopleModel,
	},
	EmployeeModel: SearchDataModel{
		SimpleDataModel{table: EmployeeModel},
	},
}
