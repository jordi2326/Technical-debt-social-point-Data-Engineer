from sqlbucket import SQLBucket

connections = {
    'sp_technical_test': 'postgresql://postgres:sp_technical_test@sp_db:5432/postgres'
}

bucket = SQLBucket(connections=connections,
                   projects_folder='src/sql_projects',
)

project = bucket.load_project(
    project_name='init_db',
    connection_name='sp_technical_test',
)

project.run()

project.run_integrity()