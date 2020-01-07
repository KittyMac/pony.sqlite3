all:
	stable env /Volumes/Development/Development/pony/ponyc/build/release/ponyc -o ./build/ ./sqlite3
	./build/sqlite3

test:
	stable env /Volumes/Development/Development/pony/ponyc/build/release/ponyc -V=0 -o ./build/ ./sqlite3
	./build/sqlite3