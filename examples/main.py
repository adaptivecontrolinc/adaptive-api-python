from datetime import datetime
from adaptive_api import ApiLive, ApiPe
from adaptive_api.pe import history_to_csv

if __name__ == "__main__":
    API_SERVER = "http://localhost"
    API_TOKEN = "123"

    client = ApiLive(API_SERVER, API_TOKEN)
    # print(client.dashboard_entries())

    # for m in client.machines():
    #     print(f"machine: {m.machine}, type: {m.type or 'N/A'}")

    client = ApiPe(API_SERVER, API_TOKEN)
    job_id = "R1010006"
    history = client.history(job_id, tags=["01.TargetTemp", "01.State"])
    if history:
        csv = history_to_csv(history)
        save_path = f"{job_id}.csv"
        with open(save_path, "w", encoding="utf-8-sig") as f:
            f.write(csv)
    
    # # Fetch program groups
    # groups = client.program_group_names()
    # print("Program groups:", groups)
    
    # # Fetch jobs and stoppages
    # jobs = client.jobs_and_stoppages(
    #     after=int(datetime.now().timestamp() * 1000) - 86400000,  # Last 24 hours
    #     before=int(datetime.now().timestamp() * 1000)
    # )
    # print(f"Found {len(jobs)} jobs and stoppages")
    
    # # Search for items
    # search_results = client.search("test", limit=10)
    # print(f"Search found {len(search_results)} results")