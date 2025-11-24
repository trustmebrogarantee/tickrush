`flask run --no-reload`

Place your migration script in versions.
`flask db upgrade`

# Create a migration after model changes
`flask db migrate -m "Describe your change"`

# Apply the migration to your database
`flask db upgrade`

# If you need to downgrade
`flask db downgrade`
