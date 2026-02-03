from datetime import datetime
from adaptiveApiLive import AdaptiveApiLive
from adaptiveApiPe import AdaptiveApiPe


if __name__ == "__main__":
    API_SERVER = "http://localhost"
    API_TOKEN = "123"

    client = AdaptiveApiLive(API_SERVER, API_TOKEN)
    print(client.dashboard_entries())

    for m in client.machines():
        print(f"machine: {m['machine']}, type: {m.get('type', 'N/A')}")

    client = AdaptiveApiPe(API_SERVER, API_TOKEN)
    
    # Fetch program groups
    groups = client.program_group_names()
    print("Program groups:", groups)
    
    # Fetch jobs and stoppages
    jobs = client.jobs_and_stoppages(
        after=int(datetime.now().timestamp() * 1000) - 86400000,  # Last 24 hours
        before=int(datetime.now().timestamp() * 1000)
    )
    print(f"Found {len(jobs)} jobs and stoppages")
    
    # Search for items
    search_results = client.search("test", limit=10)
    print(f"Search found {len(search_results)} results")
