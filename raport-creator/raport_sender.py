from dotenv import load_dotenv
import os
from jira import JIRA


def send_report(output_path: str = "report.docx"):
    load_dotenv()
    api_key = os.getenv("JIRA_KEY")
    email = os.getenv("EMAIL")
    jira = JIRA(server="https://wlasowski.atlassian.net", basic_auth=(email, api_key))

    PROJECT_NAME = "KAN"
    PROJECT_SUMMARY = "Report - Diamonds Sales"
    PROJECT_DESC = "Report about salary of diamonds"
    
    issue = jira.create_issue(project=PROJECT_NAME, summary = PROJECT_SUMMARY, description = PROJECT_DESC, issuetype={"name": "Task"})
    jira.add_attachment(issue=issue, attachment=output_path)