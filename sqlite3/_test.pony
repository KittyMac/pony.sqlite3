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
		test(_Test3)

class iso _Test1 is UnitTest
	fun name(): String => "test 1 - in memory db"

	fun apply(h: TestHelper) =>
		try
			Sqlite3.memory()?
			h.complete(true)
		else
			h.complete(false)
		end

class iso _Test2 is UnitTest
	fun name(): String => "test 2 - error test"

	fun apply(h: TestHelper) =>
		try
			Sqlite3.errorTest()?
			h.complete(false)
		else
			h.complete(true)
		end

class iso _Test3 is UnitTest
	fun name(): String => "test 3 - empty"

	fun apply(h: TestHelper) =>
		Sqlite3.empty()
		h.complete(true)
