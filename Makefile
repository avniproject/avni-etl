help:
	@IFS=$$'\n' ; \
	help_lines=(`fgrep -h "##" $(MAKEFILE_LIST) | fgrep -v fgrep | sed -e 's/\\$$//'`); \
	for help_line in $${help_lines[@]}; do \
	    IFS=$$'#' ; \
	    help_split=($$help_line) ; \
	    help_command=`echo $${help_split[0]} | sed -e 's/^ *//' -e 's/ *$$//'` ; \
	    help_info=`echo $${help_split[2]} | sed -e 's/^ *//' -e 's/ *$$//'` ; \
	    printf "%-30s %s\n" $$help_command $$help_info ; \
	done

include makefiles/utils.mk

build_jar: ## Builds the jar file
	./gradlew clean build -x test

test:
	MAVEN_OPTS="-Xmx3200m" ./gradlew clean test --stacktrace
test-server: test
test_server: test

build_server: build_jar
build-server: build_server

# <server>
start_server: build_server
	java -jar ./build/libs/etl-1.0.0-SNAPSHOT.jar

start_server_with_dump_data_org: build_server
	OPENCHS_DATABASE_NAME=avni_org OPENCHS_CLIENT_ID=dummy OPENCHS_KEYCLOAK_CLIENT_SECRET=dummy AVNI_IDP_TYPE=none java -jar ./build/libs/etl-1.0.0-SNAPSHOT.jar

debug_server: build_server
	java -Xmx2048m -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005 -jar ./build/libs/etl-1.0.0-SNAPSHOT.jar

debug_server_with_dump_data_org: build_server
	OPENCHS_DATABASE_NAME=avni_org OPENCHS_CLIENT_ID=dummy OPENCHS_KEYCLOAK_CLIENT_SECRET=dummy AVNI_IDP_TYPE=none java -Xmx2048m -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005 -jar ./build/libs/etl-1.0.0-SNAPSHOT.jar

start_server_prerelease: build_server
	-mkdir -p /tmp/avni-etl-service && sudo ln -s /tmp/avni-etl-service /var/log/avni-etl-service

	AVNI_IDP_TYPE=cognito \
	OPENCHS_CLIENT_ID=$(OPENCHS_CLIENT_ID) \
	OPENCHS_USER_POOL=$(OPENCHS_USER_POOL) \
	OPENCHS_IAM_USER=$(OPENCHS_IAM_USER) \
	OPENCHS_IAM_USER_ACCESS_KEY=$(OPENCHS_IAM_USER_ACCESS_KEY) \
	OPENCHS_IAM_USER_SECRET_ACCESS_KEY=$(OPENCHS_IAM_USER_SECRET_ACCESS_KEY) \
	OPENCHS_BUCKET_NAME=prerelease-user-media \
  	OPENCHS_DATABASE_URL=jdbc:postgresql://localhost:5433/openchs \
    	java -jar  ./build/libs/etl-1.0.0-SNAPSHOT.jar

debug_server_prerelease: build_server
	-mkdir -p /tmp/avni-etl-service && sudo ln -s /tmp/avni-etl-service /var/log/avni-etl-service

	AVNI_IDP_TYPE=cognito \
    	OPENCHS_CLIENT_ID=$(OPENCHS_CLIENT_ID) \
    	OPENCHS_USER_POOL=$(OPENCHS_USER_POOL) \
    	OPENCHS_IAM_USER=$(OPENCHS_IAM_USER) \
    	OPENCHS_IAM_USER_ACCESS_KEY=$(OPENCHS_IAM_USER_ACCESS_KEY) \
    	OPENCHS_IAM_USER_SECRET_ACCESS_KEY=$(OPENCHS_IAM_USER_SECRET_ACCESS_KEY) \
    	OPENCHS_BUCKET_NAME=prerelease-user-media \
      	OPENCHS_DATABASE_URL=jdbc:postgresql://localhost:5433/openchs \
    	java -Xmx2048m -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005 -jar ./build/libs/etl-1.0.0-SNAPSHOT.jar

start_server_staging: build_server
	-mkdir -p /tmp/avni-etl-service && sudo ln -s /tmp/avni-etl-service /var/log/avni-etl-service

	AVNI_IDP_TYPE=cognito \
	OPENCHS_CLIENT_ID=$(OPENCHS_STAGING_APP_CLIENT_ID) \
	OPENCHS_USER_POOL=$(OPENCHS_STAGING_USER_POOL_ID) \
	OPENCHS_IAM_USER=$(OPENCHS_STAGING_IAM_USER) \
	OPENCHS_IAM_USER_ACCESS_KEY=$(OPENCHS_STAGING_IAM_USER_ACCESS_KEY) \
	OPENCHS_IAM_USER_SECRET_ACCESS_KEY=$(OPENCHS_STAGING_IAM_USER_SECRET_ACCESS_KEY) \
	OPENCHS_BUCKET_NAME=staging-user-media \
  	OPENCHS_DATABASE_URL=jdbc:postgresql://localhost:5433/openchs \
    	java -jar ./build/libs/etl-1.0.0-SNAPSHOT.jar

debug_server_staging: build_server
	-mkdir -p /tmp/avni-etl-service && sudo ln -s /tmp/avni-etl-service /var/log/avni-etl-service

	AVNI_IDP_TYPE=cognito \
	OPENCHS_CLIENT_ID=$(OPENCHS_STAGING_APP_CLIENT_ID) \
	OPENCHS_USER_POOL=$(OPENCHS_STAGING_USER_POOL_ID) \
	OPENCHS_IAM_USER=$(OPENCHS_STAGING_IAM_USER) \
	OPENCHS_IAM_USER_ACCESS_KEY=$(OPENCHS_STAGING_IAM_USER_ACCESS_KEY) \
	OPENCHS_IAM_USER_SECRET_ACCESS_KEY=$(OPENCHS_STAGING_IAM_USER_SECRET_ACCESS_KEY) \
	OPENCHS_BUCKET_NAME=staging-user-media \
  	OPENCHS_DATABASE_URL=jdbc:postgresql://localhost:5433/openchs \
    	java -Xmx2048m -Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=5005 -jar ./build/libs/etl-1.0.0-SNAPSHOT.jar

boot_run:
	./gradlew bootRun

create-extensions:
	-psql -h localhost -Uopenchs openchs_test -c 'create extension if not exists "uuid-ossp"';
	-psql -h localhost -Uopenchs openchs_test -c 'create extension if not exists "ltree"';
	-psql -h localhost -Uopenchs openchs_test -c 'create extension if not exists "hstore"';

open-test-results:
	open build/reports/tests/test/index.html

start: boot_run

debug:
	./gradlew bootRun -Dagentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005

delete-etl-metadata:
ifndef schemaName
	@echo "Provde the schemaName variable"
	exit 1
endif
ifndef dbUser
	@echo "Provde the dbUser variable"
	exit 1
endif
ifndef dbOwner
	@echo "Provde the dbOwner variable"
	exit 1
endif
ifndef db
	@echo "Provde the db variable"
	exit 1
endif
	-psql -h localhost -Uopenchs $(db) -c "select delete_etl_metadata_for_schema('$(schemaName)','$(dbUser)','$(dbOwner)' )"

delete-etl-metadata-for-org-group:
ifndef schemaName
	@echo "Provde the schemaName variable"
	exit 1
endif
ifndef dbUser
	@echo "Provde the dbUser variable"
	exit 1
endif
ifndef db
	@echo "Provde the db variable"
	exit 1
endif
	-psql -h localhost -Uopenchs $(db) -c "select delete_etl_metadata_for_org('$(schemaName)', '$(dbUser)')"
