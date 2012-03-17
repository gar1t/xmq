rebar := ./rebar

compile: deps
	$(rebar) compile

quick:
	$(rebar) compile skip_deps=true

deps: rabbitmq-deps
	$(rebar) get-deps

refresh-deps: delete-deps deps
	$(rebar) get-deps

delete-deps: delete-rabbitmq-deps
	$(rebar) delete-deps

rabbitmq-deps: deps/rabbitmq-codegen \
               deps/rabbitmq-server \
               deps/rabbitmq-erlang-client \
               deps/amqp_client \
               deps/rabbit_common

deps/rabbitmq-codegen:
	cd deps; \
	git clone git://github.com/rabbitmq/rabbitmq-codegen.git --depth 1

deps/rabbitmq-server:
	cd deps; \
	git clone git://github.com/rabbitmq/rabbitmq-server.git --depth 1

deps/rabbitmq-erlang-client:
	cd deps; \
	git clone git://github.com/rabbitmq/rabbitmq-erlang-client.git \
	   --depth 1; \
	cd rabbitmq-erlang-client; \
	make

deps/amqp_client:
	cd deps; \
        ln -s rabbitmq-erlang-client amqp_client

deps/rabbit_common:
	cd deps; \
        ln -s rabbitmq-erlang-client/deps/rabbit_common-0.0.0 rabbit_common

delete-rabbitmq-deps:
	rm -rf deps/rabbitmq*

clean:
	$(rebar) clean

shell:
	erl -pa ebin $(wildcard deps/*/ebin) -s e2_reloader
