all:
	cd cmd/raftctl && go build -o raftctl main.go
	@find ./ -type f -name raftctl |xargs -i ls -alh {}
	cd example/kvstore && go build .
	@find ./ -type f -name kvstore |xargs -i ls -alh {}




clean:
	find ./ -type f -name raftctl |xargs -i rm {}