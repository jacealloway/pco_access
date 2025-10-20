import requests
import csv
import datetime
from datetime import timedelta
import time
from tqdm import tqdm
from pydantic import BaseModel
from typing import List, Optional, Dict, Tuple
from dateutil import parser

# --- API Credentials & Base URL ---
API_APP_ID = "FILL IT"
API_SECRET = "FILL IT"
BASE_URL = "https://api.planningcenteronline.com/groups/v2"

# --- Helper: safe_get with retry on 429 errors ---
def safe_get(url: str, auth: tuple, max_retries: int = 5, backoff_factor: float = 1.0) -> requests.Response:
    """
    Performs a GET request with retries. On a 429 error, waits for the time
    specified in the 'Retry-After' header (or uses exponential backoff) before retrying.
    """
    for attempt in range(max_retries):
        try:
            response = requests.get(url, auth=auth)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                if retry_after is not None:
                    wait_time = float(retry_after)
                else:
                    wait_time = backoff_factor * (2 ** attempt)
                print(f"429 received for URL {url}. Waiting for {wait_time} seconds before retrying.")
                time.sleep(wait_time)
            else:
                if attempt == max_retries - 1:
                    raise
                else:
                    time.sleep(backoff_factor * (2 ** attempt))
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise
            else:
                time.sleep(backoff_factor * (2 ** attempt))
    raise Exception(f"Failed to get URL {url} after {max_retries} attempts.")

# --- Pydantic Models ---

class Membership(BaseModel):
    person_id: str
    first_name: str
    last_name: str
    role: str

class Group(BaseModel):
    id: str
    name: str

class Event(BaseModel):
    id: str
    start_date: str

class Attendance(BaseModel):
    person_id: str
    role: str
    attended_status: bool

class Person(BaseModel):
    first_name: str
    last_name: str

# --- Coach Group Exporter (group type 448283) ---

class CoachGroupExporter:
    def __init__(self, api_app_id: str, api_secret: str):
        self.auth = (api_app_id, api_secret)
        self.group_type_url = f"{BASE_URL}/group_types/448283/groups"

    def get_coach_groups(self) -> List[Group]:
        """Retrieve all coach groups (group type 448283) as Group objects."""
        groups_list: List[Group] = []
        next_page_url: Optional[str] = self.group_type_url
        try:
            while next_page_url:
                response = safe_get(next_page_url, self.auth)
                data = response.json()
                groups = data.get("data", [])
                next_page_url = data.get("links", {}).get("next")
                for group in groups:
                    try:
                        group_id = group["id"]
                        group_name = group["attributes"]["name"]
                        groups_list.append(Group(id=group_id, name=group_name))
                    except Exception as e:
                        print(f"Error processing coach group ID {group.get('id', 'Unknown')}: {e}")
                        continue
            return groups_list
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching coach group details: {e}")
            return []

    def get_coach_group_memberships(self, group_id: str) -> List[Membership]:
        """Retrieve all membership records for a given coach group."""
        memberships: List[Membership] = []
        next_page_url: Optional[str] = f"{BASE_URL}/groups/{group_id}/members"
        try:
            while next_page_url:
                response = safe_get(next_page_url, self.auth)
                data = response.json()
                members = data.get("data", [])
                next_page_url = data.get("links", {}).get("next")
                for member in members:
                    try:
                        person_id = member["id"]
                        first_name = member["attributes"]["first_name"]
                        last_name = member["attributes"]["last_name"]
                        role = member["attributes"]["role"]
                        memberships.append(Membership(
                            person_id=person_id,
                            first_name=first_name,
                            last_name=last_name,
                            role=role
                        ))
                    except Exception as e:
                        print(f"Error processing coach member ID {member.get('id', 'Unknown')}: {e}")
                        continue
            return memberships
        except requests.exceptions.RequestException as e:
            print(f"An error occurred while fetching memberships for coach group {group_id}: {e}")
            return []

    def get_coach_group_leaders(self) -> Dict[Tuple[str, str], Dict[str, str]]:
        """
        Build a mapping from a coach leader's (member_first_name, member_last_name) tuple
        to a dictionary containing:
          - member_first_name
          - member_last_name
          - leader_first_name
          - leader_last_name
        We assume that the first membership with role "leader" in each coach group is the leader.
        """
        leader_mapping: Dict[Tuple[str, str], Dict[str, str]] = {}
        coach_groups = self.get_coach_groups()
        for group in tqdm(coach_groups, desc="Processing Coach Groups"):
            memberships = self.get_coach_group_memberships(group.id)
            leader_member = next((m for m in memberships if m.role.lower() == "leader"), None)
            if leader_member:
              for member in memberships:
                if member.role.lower() == "member":
                  key = (member.first_name, member.last_name)
                  leader_mapping[key] = {
                      "member_first_name": member.first_name,
                      "member_last_name": member.last_name,
                      "leader_first_name": leader_member.first_name,
                      "leader_last_name": leader_member.last_name
                  }
            else:
                # If no leader is found, we do not add an entry.
                pass
        return leader_mapping

# --- Planning Center Data Exporter (Connect Groups, group type 448862) ---

class PlanningCenterDataExporter:
    def __init__(self, api_app_id: str, api_secret: str):
        self.auth = (api_app_id, api_secret)

    def get_group_details(self) -> List[Group]:
        """Retrieve connect group details from group type 448862."""
        groups_list: List[Group] = []
        next_page_url = f"{BASE_URL}/group_types/448862/groups"
        try:
            while next_page_url:
                response = safe_get(next_page_url, self.auth)
                data = response.json()
                groups = data.get("data", [])
                next_page_url = data.get("links", {}).get("next")
                for group in groups:
                    try:
                        group_id = group["id"]
                        group_name = group["attributes"]["name"]
                        groups_list.append(Group(id=group_id, name=group_name))
                    except Exception as e:
                        print(f"Error processing connect group ID {group.get('id', 'Unknown')}: {e}")
                        continue
            return groups_list
        except requests.exceptions.RequestException as e:
            print(f"An error occurred retrieving connect group details: {e}")
            return []

    def get_group_events(self, group_id: str) -> List[Event]:
        """
        Retrieve the events for a given connect group that match 'Connect Group Gathering'
        and have reminders_sent.
        """
        url = f"{BASE_URL}/groups/{group_id}/events?where[name]=Connect+Group+Gathering"
        try:
            response = safe_get(url, self.auth)
            data = response.json()
            events = data.get("data", [])
            event_list: List[Event] = []
            for event in events:
                if event["attributes"].get("reminders_sent"):
                    event_id = event["id"]
                    start_date = event["attributes"]["starts_at"]
                    event_list.append(Event(id=event_id, start_date=start_date))
            return event_list
        except requests.exceptions.RequestException as e:
            print(f"Error retrieving events for group {group_id}: {e}")
            return []

    def get_group_attendance(self, event_id: str) -> List[Attendance]:
        """Retrieve attendance details for a given event."""
        url = f"{BASE_URL}/events/{event_id}/attendances"
        try:
            response = safe_get(url, self.auth)
            data = response.json()
            attendances = data.get("data", [])
            attendance_list: List[Attendance] = []
            for record in attendances:
                try:
                    person_id = record["relationships"]["person"]["data"]["id"]
                    role = record["attributes"].get("role", "")
                    attended_status = record["attributes"].get("attended", False)
                    attendance_list.append(Attendance(
                        person_id=person_id,
                        role=role,
                        attended_status=attended_status
                    ))
                except Exception as e:
                    print(f"Error processing an attendance record: {e}")
                    continue
            return attendance_list
        except requests.exceptions.RequestException as e:
            print(f"Error retrieving attendance for event {event_id}: {e}")
            return []

    def get_person_details(self, person_id: str) -> Person:
        """Retrieve a person's details by person_id."""
        url = f"{BASE_URL}/people/{person_id}"
        try:
            response = safe_get(url, self.auth)
            data = response.json()
            person_data = data.get("data", {})
            first_name = person_data.get("attributes", {}).get("first_name", "")
            last_name = person_data.get("attributes", {}).get("last_name", "")
            return Person(first_name=first_name, last_name=last_name)
        except requests.exceptions.RequestException as e:
            print(f"Error retrieving details for person {person_id}: {e}")
            return Person(first_name="", last_name="")

    def get_sunday_of_week(self, meet_date_str: str) -> str:
        """
        Given an ISO formatted meet date string (e.g., "2021-06-27T17:00:00Z"),
        compute the upcoming Sunday (i.e. the Sunday on or after the given date)
        and return it formatted as mm/dd/yyyy.
        """
        try:
            dt = datetime.datetime.strptime(meet_date_str[:10], "%Y-%m-%d").date()
            days_to_add = (6 - dt.weekday()) % 7  # 0 if already Sunday
            sunday_date = dt + timedelta(days=days_to_add)
            return sunday_date.strftime("%m/%d/%Y")
        except Exception as e:
            print(f"Error parsing meet date {meet_date_str}: {e}")
            return ""

# --- Combined Exporter ---

class CombinedExporter:
    def __init__(self, api_app_id: str, api_secret: str):
        self.coach_exporter = CoachGroupExporter(api_app_id, api_secret)
        self.pc_exporter = PlanningCenterDataExporter(api_app_id, api_secret)

    def export_to_csv(self, output_file: str):
        """
        Exports a CSV file with the columns:
          group_name, group_meet_date, sunday_date, member_first_name, member_last_name,
          member_role, attend_status, leader_first_name, leader_last_name.
        
        For each attendance record from a connect group event:
          - If the member's role is not "leader", the leader columns are set to the connect group's leader
            (determined as the first attendance record with role "Leader").
          - If the member's role is "leader", then we use the member's first and last names
            to look up their coach leader from the coach group mapping.
        """
        # Build mapping for coach group leaders: (member_first_name, member_last_name) -> {leader_first_name, leader_last_name, ...}
        coach_leaders = self.coach_exporter.get_coach_group_leaders()
        # Fetch connect groups
        connect_groups = self.pc_exporter.get_group_details()
        if not connect_groups:
            print("No connect group details retrieved.")
            return

        with open(output_file, mode="w", newline="", encoding="utf-8") as csvfile:
            fieldnames = [
                "group_name",
                "group_meet_date",
                "sunday_date",
                "member_first_name",
                "member_last_name",
                "member_role",
                "attend_status",
                "leader_first_name",
                "leader_last_name"
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()

            for group in tqdm(connect_groups, desc="Processing Connect Groups"):
                group_name = group.name
                events = self.pc_exporter.get_group_events(group.id)
                for event in events:
                    meet_date_str = event.start_date
                    try:
                        dt = datetime.datetime.strptime(meet_date_str[:10], "%Y-%m-%d").date()
                        group_meet_date = dt.strftime("%m/%d/%Y")
                    except Exception as e:
                        print(f"Error formatting meet date {meet_date_str}: {e}")
                        group_meet_date = ""
                    sunday_date = self.pc_exporter.get_sunday_of_week(meet_date_str)
                    attendances = self.pc_exporter.get_group_attendance(event.id)
                    
                    # Determine connect group leader (first attendance with role "Leader")
                    connect_leader: Optional[Tuple[str, str]] = None
                    for att in attendances:
                        if att.role.lower() == "leader":
                            leader_person = self.pc_exporter.get_person_details(att.person_id)
                            connect_leader = (leader_person.first_name, leader_person.last_name)
                            break

                    for attendance in attendances:
                        person = self.pc_exporter.get_person_details(attendance.person_id)
                        member_first = person.first_name
                        member_last = person.last_name
                        member_role = attendance.role
                        attend_status = attendance.attended_status
                        # Determine leader columns:
                        # For members with role "leader", use their first and last names to look up the coach leader.
                        if member_role.lower() == "leader":
                            key = (member_first, member_last)
                            if key in coach_leaders:
                                leader_info = coach_leaders[key]
                                leader_first = leader_info["leader_first_name"]
                                leader_last = leader_info["leader_last_name"]
                            else:
                                leader_first, leader_last = connect_leader if connect_leader else ("", "")
                        else:
                            leader_first, leader_last = connect_leader if connect_leader else ("", "")
                        
                        writer.writerow({
                            "group_name": group_name,
                            "group_meet_date": group_meet_date,
                            "sunday_date": sunday_date,
                            "member_first_name": member_first,
                            "member_last_name": member_last,
                            "member_role": member_role,
                            "attend_status": attend_status,
                            "leader_first_name": leader_first,
                            "leader_last_name": leader_last
                        })
        print(f"Data exported successfully to {output_file}")

if __name__ == "__main__":
    exporter = CombinedExporter(api_app_id=API_APP_ID, api_secret=API_SECRET)
    exporter.export_to_csv("planning_center_data.csv")