# run_simple.py - Simplest working version
import os
import sys
import requests
from dotenv import load_dotenv
from dagster import materialize
from energy_pipeline_incremental import defs

load_dotenv()

def send_slack(message, is_error=False):
    webhook = os.environ.get("SLACK_WEBHOOK_URL")
    if webhook:
        icon = "❌" if is_error else "✅"
        requests.post(webhook, json={"text": f"{icon} {message}"}, timeout=10)

if __name__ == "__main__":
    send_slack("Pipeline execution started")
    
    try:
        result = materialize(list(defs.assets))
        
        if result.success:
            send_slack("Pipeline succeeded! Check BigQuery for new data")
            print("✅ Pipeline succeeded - Slack notification sent")
        else:
            send_slack("Pipeline failed - check logs", is_error=True)
            print("❌ Pipeline failed - Slack notification sent")
            sys.exit(1)
            
    except Exception as e:
        send_slack(f"Pipeline crashed: {str(e)[:100]}", is_error=True)
        print(f"❌ Pipeline crashed: {e}")
        sys.exit(1)