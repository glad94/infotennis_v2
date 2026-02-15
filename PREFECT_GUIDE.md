# Prefect Local Server Guide

## Quick Start: See Your Pipeline in the UI

### Step 1: Start Prefect Server

```bash
cd "c:\Users\lgjg1\Documents\Random Projects\Tennis\infotennis_v2"
.venv\Scripts\activate
prefect server start
```

Open browser to: **http://127.0.0.1:4200**

---

### Step 2: Run the Flow (Creates Flow Run in UI)

Open a **NEW terminal**:

```bash
cd "c:\Users\lgjg1\Documents\Random Projects\Tennis\infotennis_v2"
.venv\Scripts\activate
python flows/atp_main_flow.py --year 2024
```

You'll immediately see the flow run appear in the UI at: **Flows → ATP Results Archive Pipeline**

---

### Step 3 (Optional): Create a Deployment

To make the flow **schedulable** and visible in the Deployments tab:

```bash
# Create deployment
prefect deploy flows/atp_main_flow.py:atp_results_archive_flow \
    --name "atp-daily" \
    --pool "default-pool"

# Start a worker to execute scheduled runs
prefect worker start --pool "default-pool"

# Trigger via UI or CLI
prefect deployment run "ATP Results Archive Pipeline/atp-daily" --param year=2024
```

---

## Understanding Prefect Orchestration

### Implicit Dependencies (How Prefect Works)

```python
@flow
def my_flow():
    # Sequential execution (data dependency)
    result1 = task_a()
    result2 = task_b(result1)  # Waits for task_a
    result3 = task_c(result2)  # Waits for task_b
```

**Prefect automatically detects**:
- Task B depends on Task A (uses its output)
- Task C depends on Task B (uses its output)
- Creates dependency graph: A → B → C

### Parallel Execution

```python
from prefect import flow, task

@flow
def parallel_flow():
    # Submit tasks to run in parallel
    future_a = task_a.submit()
    future_b = task_b.submit()
    
    # Wait for both to complete
    result_a = future_a.result()
    result_b = future_b.result()
    
    # Combine results
    final = task_c(result_a, result_b)
```

---

## UI Features

Once you run the flow, you can view in the UI:

- **Flow Runs** tab: See all executions
- **Task Run Timeline**: Visual graph of task execution
- **Logs**: Detailed logs for each task
- **Artifacts**: Any artifacts created during the run

---

## Environment Variables on Prefect Server

When running locally, Prefect automatically picks up your `.env` file via `load_dotenv()`.

For **Prefect Cloud** or **production deployments**:

1. **Work Pool** environment variables:
   ```bash
   prefect work-pool set-variable --pool "default-pool" AWS_ACCESS_KEY_ID "your-key"
   ```

2. **Prefect Blocks** (recommended for secrets):
   ```python
   from prefect.blocks.system import Secret
   
   Secret(value="my-secret").save("motherduck-token")
   ```

3. **Deployment-level variables**:
   ```bash
   prefect deployment set-variable "deployment-name" AWS_REGION "us-east-1"
   ```
