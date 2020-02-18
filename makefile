all:
	corral run -- ponyc --print-code -o ./build/ ./sqlite3
	./build/sqlite3

test:
	corral run -- ponyc -V=0 -o ./build/ ./sqlite3
	./build/sqlite3




corral-fetch:
	@corral clean -q
	@corral fetch -q

corral-local:
	-@rm corral.json
	-@rm lock.json
	@corral init -q
	@corral add /Volumes/Development/Development/pony/pony.stringExt -q

corral-git:
	-@rm corral.json
	-@rm lock.json
	@corral init -q
	@corral add github.com/KittyMac/pony.stringExt.git -q

ci: corral-git corral-fetch all
	
dev: corral-local corral-fetch all
	