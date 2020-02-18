use "ponytest"
use "stringExt"

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
      let db = PonySqlite3.memory()?
      
      db.beginTransaction()?
      
      let createTableString = """
          create table users (
            id    integer primary key,
            name  text  not null,
            age   int   not null check (age > 0),
            email text  not null
          )
        """
      
      db.exec(createTableString)?
      
      let insertPeopleString = """
              insert into users
              (name, age, email)
              values
              (?, ?, ?),
              (?, ?, ?),
              (?, ?, ?)
        """     
      let insertPeopleSql = db.sql(insertPeopleString)?.>
        bind("tim").>bind(I32(40)).>bind("tim@example.com").>
        bind("anika").>bind(I32(20)).>bind("anika@example.com").>
        bind("anders").>bind(I32(30)).>bind("anders@example.com")
      
      db.query(insertPeopleSql)?.finish()?
      
      
      let getPeopleString = """
              select * from users
        """
      var resultCheck = String(1024)
      for row in db.query(getPeopleString)? do
        resultCheck.append(row.string(1)?)
        resultCheck.push(',')
        resultCheck.append(row.i32(2)?.string())
        resultCheck.push(',')
        resultCheck.append(row.string(3)?)
        resultCheck.push(',')
      end
      
      db.endTransaction()?
            
      h.complete( resultCheck == "tim,40,tim@example.com,anika,20,anika@example.com,anders,30,anders@example.com," )
    else
      h.complete( false )
    end

class iso _Test2 is UnitTest
  fun name(): String => "test 2 - empty"

  fun apply(h: TestHelper) =>
    PonySqlite3.empty()
    h.complete(true)
