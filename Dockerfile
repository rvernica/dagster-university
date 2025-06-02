FROM python:3.10-slim

RUN pip install \
    dagster \
    dagster-postgres \
    dagster-docker \
    geopandas \
    matplotlib \
    dagster_duckdb

# Add code location code
WORKDIR /opt/dagster/app
COPY dagster_university/dagster_essentials /opt/dagster/app

# Run dagster code server on port 4000
EXPOSE 4000

# CMD allows this to be overridden from run launchers or executors to execute runs and steps
CMD ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-f", "dagster_essentials/definitions.py"]
