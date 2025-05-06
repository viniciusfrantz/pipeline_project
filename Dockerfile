FROM astrocrpublic.azurecr.io/runtime:3.0-1

# replace dbt-postgres with another supported adapter if you're using a different warehouse type
RUN python -m venv dbt_venv && \
    source dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-snowflake && \
    deactivate

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY .dbt /home/astro/.dbt