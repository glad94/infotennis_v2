from tasks.ingestion.get_atp_calendar import get_atp_results_archive_task

if __name__ == "__main__":
    # You can specify a year, or leave blank for current year
    result = get_atp_results_archive_task.fn(2024)  # .fn runs the underlying function synchronously
    print(result)