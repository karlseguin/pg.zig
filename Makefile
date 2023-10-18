.PHONY: t
t:
	zig build test --summary all -freference-trace

.PHONY: .d
d:
	docker run -p 5432:5432 -it --rm \
		-v $(shell pwd)/tests/pg_hba.conf:/etc/postgresql/pg_hba.conf \
		-e POSTGRES_PASSWORD=root_pw \
		-e POSTGRES_USER=postgres \
		pgzig:pg \
		postgres \
			-c 'hba_file=/etc/postgresql/pg_hba.conf'
