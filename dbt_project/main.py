from fastapi import FastAPI, HTTPException
import subprocess

app = FastAPI()

@app.get("/dbt-debug")
def dbt_debug():
    result = subprocess.run(["dbt", "debug"], capture_output=True, text=True)
    if result.returncode != 0:
        raise HTTPException(
            status_code=500,
            detail={"stdout": result.stdout, "stderr": result.stderr, "code": result.returncode}
        )
    return {"stdout": result.stdout, "stderr": result.stderr, "code": result.returncode}


@app.post("/dbt-run")
def dbt_run():
    """Uruchom dbt run"""
    result = subprocess.run(["dbt", "run"], capture_output=True, text=True)
    return {"stdout": result.stdout, "stderr": result.stderr, "code": result.returncode}
