use "ponytest"
use "fileExt"
use "stringExt"
use "json"

actor Main is TestList
	new create(env: Env) => PonyTest(env, this)
	new make() => None

	fun tag tests(test: PonyTest) =>
		test(_Test1)
		test(_Test2)

class iso _Test1 is UnitTest
	fun name(): String => "test 1 - in memory db"

	fun apply(h: TestHelper) =>
		try
			let db = Sqlite3.memory()?
			
			let createTable = """
				  create table users (
				    id    integer primary key,
				    name  text  not null,
				    age   int   not null check (age > 0),
				    email text  not null
				  )
				"""
			
			db.query(createTable)?.finish()?
			
			let insertPeople = """
			        insert into users
			        (name, age, email)
			        values
			        ("tim", 40, "tim@example.com"),
			        ("anika", 20, "anika@example.com"),
			        ("anders", 30, "anders@example.com")
				"""
			
			db.query(insertPeople)?.finish()?
			
			
			let getPeople = """
			        select * from users
				"""
			
			var resultCheck = String(1024)
			for row in db.query(getPeople)? do
				resultCheck.append(row(1)?)
				resultCheck.push(',')
				resultCheck.append(row(2)?)
				resultCheck.push(',')
				resultCheck.append(row(3)?)
				resultCheck.push(',')
			end
						
			h.complete( resultCheck == "tim,40,tim@example.com,anika,20,anika@example.com,anders,30,anders@example.com," )
		else
			h.complete( false )
		end

class iso _Test2 is UnitTest
	fun name(): String => "test 2 - empty"

	fun apply(h: TestHelper) =>
		Sqlite3.empty()
		h.complete(true)
