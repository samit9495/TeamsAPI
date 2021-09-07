import datetime
import errno
import json
import logging
import os
import time
import warnings
import pymysql
import pytz
import requests
import ast


class MicrosoftTeams:
    def __init__(self):
        self.auth = self.get_app_auth()
        self.owners = config_data["OWNERS"]
        self.strt()

    def strt(self):
        logger.info("Starting...")
        self.adgroups = self.get_group()
        digiclasses = self.digi_class_data()

        for cls in set(self.adgroups.values()).intersection(set(digiclasses.keys())):  # Already Present
            print("Already Present:", cls)
            logger.info(f"Already Present {cls}")
            dset = set()
            dset.update([x[0] for x in digiclasses[cls]["Trainer"]])
            dset.update([x[0] for x in digiclasses[cls]["Students"]])
            gid = None
            for k, v in self.adgroups.items():
                if v == cls:
                    gid = k
            if gid:
                gset = self.get_group_members(gid)
            else:
                continue
            self.adusers = self.get_users()
            mem = []
            onr = []
            if len(dset - gset) > 0 or len(gset - dset) > 0:
                logger.info(f"Group users: {str(gset)}")
                logger.info(f"digicomp users: {str(dset)}")

            for usr in dset - gset:  # add users
                print("adding users", usr)
                logger.info(f"Adding user: {usr}")
                if usr not in self.adusers.keys():
                    self.guest_invite(usr)
                    print(f"Sending guest invite to {usr}")
                    logger.info(f"Sending guest invite to: {usr}")
                time.sleep(10)

                self.adusers = self.get_users()
                self.send_mail(usr,["fiona.brandt@digicomp.ch", "tsfb@digicomp.ch"])
                self.addmember(gid, self.adusers.get(usr))
                for x in digiclasses[cls]["Trainer"]:
                    if usr in x:
                        onr.append(x)
                for x in digiclasses[cls]["Students"]:
                    if usr in x:
                        mem.append(x)

            tdata = self.get_teams(gid)
            if tdata == "200":
                logger.info(f"tdata....{tdata}")
                self.grpname = ""
                for i in self.dclasses:
                    if i["AUSSFId"] == cls:
                        self.grpname = i["AUSTitel"]
                allusers = []
                [allusers.append(x[0]) for x in digiclasses[cls]["Trainer"]]
                [allusers.append(x[0]) for x in digiclasses[cls]["Students"]]

                for trm in self.dterms:
                    joinUrl = self.get_table_Data("invite_url", whr=["Termid", trm["TERMSFId"]])

                    if cls in trm.values():
                        try:
                            if joinUrl[0][0] == "None":
                                print("invite URL is not present. Sending invite again")
                                logger.info("invite URL is not present. Sending invite again")
                                subject = f"{self.grpname}: {trm['TERMVon']} CET"
                                iurl, evd = self.online_meeting(trm["TERMVon"], trm["TERMBis"],
                                                                digiclasses[cls]["Trainer"],
                                                                digiclasses[cls]["Students"], subject)
                                self.update_table_url([iurl, self.get_ts(), evd[0], evd[1]],
                                                      whr=["Termid ", trm["TERMSFId"]])
                        except IndexError:
                            print(f"New Term {trm['TERMSFId']} added. Sending invite again")
                            logger.info(f"New Term {trm['TERMSFId']} added. Sending invite again")
                            subject = f"{self.grpname}: {trm['TERMVon']} CET"
                            iurl, evd = self.online_meeting(trm["TERMVon"], trm["TERMBis"],
                                                            digiclasses[cls]["Trainer"],
                                                            digiclasses[cls]["Students"], subject)
                            self.insert_msdata(
                                [trm["TERMSFId"], cls, str(allusers), gid, self.get_ts(), trm["TERMVon"],
                                 trm["TERMBis"], iurl, evd[0], evd[1]])

            if tdata != "200":
                print("Team not present. creating team.")
                logger.info(f"Team not present under group: {gid}. Creating Team")
                try:
                    res = self.create_team(gid)
                    if res == "success":
                        self.grpname = ""
                        for i in self.dclasses:
                            if i["AUSSFId"] == cls:
                                self.grpname = i["AUSTitel"]
                        allusers = []
                        [allusers.append(x[0]) for x in digiclasses[cls]["Trainer"]]
                        [allusers.append(x[0]) for x in digiclasses[cls]["Students"]]

                        for trm in self.dterms:
                            if cls in trm.values():
                                subject = f"{self.grpname}: {trm['TERMVon']} CET"
                                iurl, event_details = self.online_meeting(trm["TERMVon"], trm["TERMBis"],
                                                                          digiclasses[cls]["Trainer"],
                                                                          digiclasses[cls]["Students"], subject)
                                self.insert_msdata(
                                    [trm["TERMSFId"], cls, str(allusers), gid, self.get_ts(), trm["TERMVon"],
                                     trm["TERMBis"], iurl, event_details[0], event_details[1]])

                except Exception as e:
                    logger.exception(f"Exception in creatint team under group:{gid}.>>> {e}")
                    print("Exception", e)

            if len(mem) > 0 or len(onr) > 0:
                print(f"New users found in {cls}. adding...")
                logger.info(f"New users found in {cls}. adding...")
                for item in self.dterms:
                    if item["TERMKey"] == cls:
                        grp = ""
                        for x in self.dclasses:
                            if x["AUSSFId"] == cls:
                                grp = x["AUSTitel"]

                        sub = f"{grp}: {item['TERMVon']} CET"
                        joinUrl = self.get_table_Data("invite_url", whr=["Termid", item["TERMSFId"]])
                        if len(joinUrl) > 0 and joinUrl[0][0] != 'None':
                            joinUrl = joinUrl[0][0]
                            dbusers = self.get_table_Data("Users", ["Termid", item["TERMSFId"]])
                            dbuserslist = set()
                            [dbuserslist.update(ast.literal_eval(i[0])) for i in dbusers]
                            dbuserslist = list(dbuserslist)
                            for x in mem:
                                dbuserslist.append(x[0])
                            for x in onr:
                                dbuserslist.append(x[0])
                            res, dt = self.create_event(joinUrl, item["TERMVon"], item["TERMBis"], mem, onr, sub)
                            if res == "success":
                                prev_data = self.get_table_multi_column(["Call_uid", "Event_id"],
                                                                        ["Termid", item["TERMSFId"]])
                                dt[0].extend(ast.literal_eval(prev_data[0][0]))
                                dt[1].extend(ast.literal_eval(prev_data[0][1]))
                                self.update_table_usersandevent([dbuserslist, self.get_ts(), dt[0], dt[1]],
                                                                ["Termid", item["TERMSFId"]])
                        else:
                            logger.info("No joinUrl found")
                            print("No JoinUrl found")

            for usr in gset - dset:  # remove users
                if usr not in self.owners.keys():
                    print(f"Deleting user: {usr} from class: {cls}")
                    logger.info(f"Deleting user: {usr} from class: {cls}")
                    self.delete_user(gid, self.adusers[usr])
                    dbusers = self.get_table_Data("Users", ["Classes", cls])
                    dbuserslist = set()
                    [dbuserslist.update(ast.literal_eval(i[0])) for i in dbusers]
                    dbuserslist = list(dbuserslist)
                    try:
                        dbuserslist.remove(usr)
                        self.update_table_users(dbuserslist, ["Classes", cls])
                    except ValueError:
                        pass

            self.adgroups = self.get_group()

        for cls in set(digiclasses.keys()).difference(set(self.adgroups.values())):  # Add new Group
            print(f"adding new group {cls}")
            logger.info(f"Adding new group: {cls}")
            self.adusers = self.get_users()
            allusers = []
            [allusers.append(x[0]) for x in digiclasses[cls]["Trainer"]]
            [allusers.append(x[0]) for x in digiclasses[cls]["Students"]]
            self.grpname = ""
            for i in self.dclasses:
                if i["AUSSFId"] == cls:
                    self.grpname = i["AUSTitel"]

            for usr in allusers:
                if usr not in self.adusers.keys():
                    self.guest_invite(usr)
                    print("sending guest invite to", usr)
                    logger.info(f"Sending guest invite to user:{usr}")
            print("Creating Team...")
            logger.info(f"Creating team...")
            time.sleep(20)
            self.adusers = self.get_users()
            st_grp2 = []
            if len(self.owners.keys()) + len(digiclasses[cls]["Trainer"]) + len(digiclasses[cls]["Students"]):
                st_grp1 = digiclasses[cls]["Students"][
                          :19 - (len(self.owners.keys()) + len(digiclasses[cls]["Trainer"]))]
                st_grp2 = digiclasses[cls]["Students"][
                          19 - (len(self.owners.keys()) + len(digiclasses[cls]["Trainer"])):]
            else:
                st_grp1 = digiclasses[cls]["Students"]

            result, grp_id = self.create_group(self.grpname, cls, st_grp1,
                                               digiclasses[cls]["Trainer"])
            if result == "success":
                for usr in allusers:
                    self.send_mail(usr, ["fiona.brandt@digicomp.ch", "tsfb@digicomp.ch"])

                for x in st_grp2:
                    print(f"adding member to {grp_id}: {x[0]}")
                    logger.info(f"adding member to {grp_id}: {x[0]}")
                    self.addmember(grp_id, self.adusers.get(x[0]))

                logger.info(f"Group created: {grp_id}")
                print(f"group  created. Group id:{grp_id}")
                for trm in self.dterms:
                    if cls in trm.values():
                        subject = f"{self.grpname}: {trm['TERMVon']} CET"
                        iurl, event_details = self.online_meeting(trm["TERMVon"], trm["TERMBis"],
                                                                  digiclasses[cls]["Trainer"],
                                                                  digiclasses[cls]["Students"], subject)
                        self.insert_msdata(
                            [trm["TERMSFId"], cls, str(allusers), grp_id, self.get_ts(), trm["TERMVon"], trm["TERMBis"],
                             iurl, event_details[0], event_details[1]])

            else:
                print("Error in creating Group.")
                logger.info(f"Error creating group")
            self.adgroups = self.get_group()


        flag = False
        db_clsng = dict(self.get_table_multi_column(["Classes", "Group_id"]))
        for cls in set(self.adgroups.values()).difference(set(digiclasses.keys())):  # Remove Group
            if cls in db_clsng.keys():
                print(f"Removing Team: {cls}")
                logger.info(f"Removing Team {cls}")
                self.delete_group(db_clsng.get(cls))
                self.update_table(self.get_ts(), whr=["Classes", cls])
                self.adgroups = self.get_group()
                flag = True

        for cls in digiclasses.keys():  # Remove Group
            data = self.get_classwise_data(self.dclasses, cls)
            if data.get("AUSStatus") == "Abgeschlossen":
                print(f"Removing Team: {cls}")
                logger.info(f"Removing Team {cls}, based of class status")
                self.delete_group(db_clsng.get(cls))
                self.update_table(self.get_ts(), whr=["Classes", cls])
                self.adgroups = self.get_group()
                flag = True

        if flag:
            self.adusers = self.get_users()
            self.digiusers = self.get_digi_users()
            digilist = [x["TeilnehmerEmail"] for x in self.digiusers]

            for u in set(self.adusers.keys()):
                if u not in digilist:
                    print("removing user", u)
                    logger.info(f"removing user :{u}")
                    self.delete_guest_users(self.adusers[u])


        for trm in self.dterms:  # change of date
            dbdata = self.get_table_multi_column(
                ["Classes", "Group_id", "Time_from", "Time_to", "Call_uid", "Event_id"], ["Termid", trm["TERMSFId"]])
            if len(dbdata) >= 1:
                dbdata = dbdata[0]
                grpname = ""
                for i in self.dclasses:
                    if i["AUSSFId"] == dbdata[0]:
                        grpname = i["AUSTitel"]
                subject = f"{grpname}: {trm['TERMVon']} CET"
                d1 = datetime.datetime.strptime(trm["TERMVon"], "%Y-%m-%d %H:%M:%S")
                d2 = datetime.datetime.strptime(trm["TERMBis"], "%Y-%m-%d %H:%M:%S")
                if dbdata[2] != d1 or dbdata[3] != d2:
                    print("Datetime does not match.sending update")
                    logger.info("Datetime does not match.sending update")
                    event_ids = ast.literal_eval(dbdata[5])
                    call_ids = ast.literal_eval(dbdata[4])
                    for i, x in enumerate(event_ids):
                        self.update_event(x, call_ids[i], trm["TERMVon"], trm["TERMBis"], subject)
                    self.update_table_time_to_n_from([trm["TERMVon"], trm["TERMBis"]], ["Termid", trm["TERMSFId"]])



        for cls in digiclasses.keys():
            dbdata = self.get_table_multi_column(["Termid", "Event_id", "Deleted_on"], ["Classes", cls])
            db_set = {x[0] for x in dbdata}
            digi_set = {x["TERMSFId"] for x in self.dterms if x["TERMKey"] == cls}
            for x in db_set.difference(digi_set):
                for y in dbdata:
                    if x == y[0] and not y[2]:
                        print("Deleting event", x)
                        logger.info(f"Deleting event: {x}")
                        for item in ast.literal_eval(y[1]):
                            res = self.delete_event(item)
                            if res == "204":
                                self.update_table(self.get_ts(), ["Termid", y[0]])

    def get_classwise_data(self, data, cl):
        for x in data:
            if x["AUSSFId"] == cl:
                return x


    def delete_guest_users(self,uid):
        url = f"https://graph.microsoft.com/v1.0/users/{uid}"
        headers = {
            "Authorization": f"Bearer {self.auth}",
            "Content-Type": "application/json",
        }
        res = requests.delete(url,headers=headers)


    def delete_event(self, e_id):
        u_id = self.adusers.get(config_data["SENDER_ID"])
        url = f"https://graph.microsoft.com/v1.0/users/{u_id}/calendar/events/{e_id}"

        headers = {
            "Authorization": f"Bearer {self.auth}",
        }

        res = requests.delete(url, headers=headers)
        return str(res.status_code)

    def update_event(self, e_id, call_id, date1, date2, sub):
        u_id = self.adusers.get(config_data["SENDER_ID"])
        url = f"https://graph.microsoft.com/v1.0/users/{u_id}/calendar/events/{e_id}"
        headers = {
            "Authorization": f"Bearer {self.auth}",
            "Content-Type": "application/json",
            'Prefer': 'outlook.timezone="Central Europe Standard Time"'
        }
        offset = self.to_cet(date1)
        date1 = date1.split()
        date1 = f"{date1[0]}T{date1[1]}+{offset}"
        date2 = date2.split()
        date2 = f"{date2[0]}T{date2[1]}+{offset}"

        data = {
            "start": {"dateTime": date1, "timeZone": "Central Europe Standard Time"},
            "end": {"dateTime": date2, "timeZone": "Central Europe Standard Time"},
            "subject": sub,
            "recurrence": None,
            "iCalUId": call_id,
            "reminderMinutesBeforeStart": 99,
            "isReminderOn": True
        }

        data = json.dumps(data)
        res = requests.patch(url, data, headers=headers)

    def send_mail(self, email, replyto):
        meauth = self.get_me_auth()
        url = f"https://graph.microsoft.com/v1.0/me/sendMail"
        rtlist = []
        for x in replyto:
            rtlist.append({"emailAddress": {"address": x}}, )

        headers = {
            "Authorization": f"Bearer {meauth}",
            "Content-Type": "application/json",
        }
        dd2 = {
            "message": {
                "subject": "Your Access link to your Digiteams Class Channel",
                "body": {
                    "contentType": "Text",
                    "content": """Dear Participant of the Digicomp Live-virtual Training

    We welcome you to the Digiteams (Microsoft Teams) Learning Platform. To get access to your class channel please perform the following steps:

        1. Accept the Microsoft Teams Invitation sent to you in the other mail
        2. Click on this link to get access to the class channel (use Google Chrome Browser): https://teams.microsoft.com?tenantId=4b6706bb-093e-4748-b8cb-a1c012d59258
        3. Accept all calendar invites so you have the session links in your calendar
    
    Important: 

        • Once you do access the first time after some minutes you will see Digiteams as a guest organisation in your Microsoft Teams. 
        • If you use then the Microsoft Teams app please logout and login again so the app does the sync too
    
    In case you have a problem accessing the DigiTeams platform please reply to this mail and let us know.



    We wish you a great learning experience

    Digicomp Training Team
    
    """
                },
                "toRecipients": [
                    {
                        "emailAddress": {
                            "address": email,
                        }
                    }
                ],
                "replyTo": rtlist
            }
        }
        res = requests.post(url, headers=headers, data=json.dumps(dd2), verify=False)

    def get_group(self):
        url = f"https://graph.microsoft.com/v1.0/groups"
        headers = {
            "Authorization": f"Bearer {self.auth}",
            "Content-Type": "application/json",
        }
        res = requests.get(url, headers=headers)
        data = json.loads(res.text)
        link = data.get("@odata.nextLink")
        data = data.get("value")
        while link:
            res = requests.get(link, headers=headers)
            data2 = json.loads(res.text)
            link = data2.get("@odata.nextLink")
            data.extend(data2.get("value"))
        d = {}
        for x in data:
            if x.get("mailNickname"):
                d[x.get("id")] = x.get("mailNickname")
        return d

    def delete_user(self, gid, mid):
        url = f"https://graph.microsoft.com/v1.0/groups/{gid}/members/{mid}/$ref"
        headers = {
            "Authorization": f"Bearer {self.auth}",
        }
        res = requests.delete(url, headers=headers)

    def db_connect(self):
        self.db = pymysql.connect(db_host, db_user, db_password,
                                  db_name)
        cur = self.db.cursor()
        return cur

    def get_table_Data(self, col, whr=None):
        cursor = self.db_connect()
        if whr:
            cursor.execute(f"SELECT {col} FROM DATA where {whr[0]} = '{whr[1]}';")
        else:
            cursor.execute(f"SELECT {col} FROM DATA;")
        rows = cursor.fetchall()
        self.db.close()
        if len(list(set(rows))) > 0:
            return list(set(rows))
        else:
            return []

    def update_table_users(self, data, whr=None):
        cursor = self.db_connect()
        data = str(data).replace("'", '"')
        if whr:
            logger.info(
                f"UPDATE DATA SET Users = '{str(data)}', Updated_on = '{self.get_ts()}' where {whr[0]} = '{whr[1]}';")
            cursor.execute(
                f"UPDATE DATA SET Users = '{str(data)}', Updated_on = '{self.get_ts()}' where {whr[0]} = '{whr[1]}';")
        else:
            logger.info(f"UPDATE DATA SET Users = '{str(data)}' Updated_on = '{self.get_ts()}';")
            cursor.execute(f"UPDATE DATA SET Users = '{str(data)}' Updated_on = '{self.get_ts()}';")
        self.db.commit()
        self.db.close()

    def update_table_time_to_n_from(self, data, whr=None):
        cursor = self.db_connect()
        if whr:
            logger.info(
                f"UPDATE DATA SET Time_from = '{str(data[0])}',Time_to = '{str(data[1])}' where {whr[0]} = '{whr[1]}';")
            cursor.execute(
                f"UPDATE DATA SET Time_from = '{str(data[0])}',Time_to = '{str(data[1])}' where {whr[0]} = '{whr[1]}';")
        else:
            logger.info(f"UPDATE DATA SET Time_from = '{str(data[0])}',Time_to = '{str(data[1])}';")
            cursor.execute(f"UPDATE DATA SET Time_from = '{str(data[0])}',Time_to = '{str(data[1])}';")
        self.db.commit()
        self.db.close()

    def update_table_usersandevent(self, data, whr=None):
        cursor = self.db_connect()
        for i in range(len(data)):
            data[i] = str(data[i]).replace("'", '"')
        # data = str(data).replace("'", '"')
        if whr:
            logger.info(
                f"UPDATE DATA SET Users = '{str(data[0])}', Updated_on = '{str(data[1])}', Call_uid='{data[2]}', Event_id='{data[3]}' where {whr[0]} = '{whr[1]}';")
            cursor.execute(
                f"UPDATE DATA SET Users = '{str(data[0])}', Updated_on = '{str(data[1])}', Call_uid='{data[2]}', Event_id='{data[3]}' where {whr[0]} = '{whr[1]}';")
        else:
            logger.info(
                f"UPDATE DATA SET Users = '{str(data[0])}' Updated_on = '{str(data[1])}', Call_uid='{data[2]}', Event_id='{data[3]}';")
            cursor.execute(
                f"UPDATE DATA SET Users = '{str(data[0])}' Updated_on = '{str(data[1])}', Call_uid='{data[2]}', Event_id='{data[3]}';")
        self.db.commit()
        self.db.close()

    def update_table(self, data, whr=None):
        cursor = self.db_connect()
        data = str(data).replace("'", '"')
        if whr:
            logger.info(f"UPDATE DATA SET Deleted_on = '{str(data)}' where {whr[0]} = '{whr[1]}';")
            cursor.execute(f"UPDATE DATA SET Deleted_on = '{str(data)}' where {whr[0]} = '{whr[1]}';")
        else:
            logger.info(f"UPDATE DATA SET Deleted_on = '{str(data)}';")
            cursor.execute(f"UPDATE DATA SET Deleted_on = '{str(data)}';")
        self.db.commit()
        self.db.close()

    def update_table_url(self, data, whr=None):
        cursor = self.db_connect()
        if whr:
            logger.info(
                f"UPDATE DATA SET invite_url = '{data[0]}',Updated_on='{data[1]}', Call_uid='{data[2]}', Event_id='{data[3]}' where {whr[0]} = '{whr[1]}';")
            cursor.execute(
                f"UPDATE DATA SET invite_url = '{data[0]}',Updated_on='{data[1]}', Call_uid='{data[2]}', Event_id='{data[3]}' where {whr[0]} = '{whr[1]}';")
        else:
            logger.info(
                f"UPDATE DATA SET invite_url = '{data[0]}',Updated_on='{data[1]}', Call_uid='{data[2]}', Event_id='{data[3]}';")
            cursor.execute(
                f"UPDATE DATA SET invite_url = '{data[0]}',Updated_on='{data[1]}', Call_uid='{data[2]}', Event_id='{data[3]}';")
        self.db.commit()
        self.db.close()

    def get_table_multi_column(self, cols, whr=None):
        cursor = self.db_connect()
        if whr:
            cursor.execute(f"SELECT {','.join(cols)} FROM DATA where {whr[0]} = '{whr[1]}';")
        else:
            cursor.execute(f"SELECT {','.join(cols)} FROM DATA;")
        rows = cursor.fetchall()
        self.db.close()
        if len(list(set(rows))) > 0:
            return list(set(rows))
        else:
            return []

    def insert_msdata(self, data):
        cursor = self.db_connect()
        sql = f"""INSERT INTO DATA(Termid, Classes, Users, Group_id, Created_on,Time_from, Time_to, invite_url, Call_uid, Event_id)
               VALUES ("""
        # sql = f"""INSERT IGNORE INTO DATA(Termid, Classes, Users, Created_on, Updated_on, Deleted_on)
        #        VALUES ("""
        st = ""
        for x in data:
            st += f"""'{str(x).replace("'", '"')}', """
        st = st[:-2] + ");"
        sql += st
        logger.info(str(sql))
        try:
            with warnings.catch_warnings():
                # warnings.simplefilter("ignore")
                cursor.execute(sql)
                self.db.commit()
                print("success")
                logger.info(f"Data inserted successfully")
        except Exception as e:
            self.db.rollback()
            print("rolling back", e)
            logger.info(f"Data Insertion failed. Rolling back. Exception: {e}")

    def addmember(self, gid, member):
        url = f"https://graph.microsoft.com/v1.0/groups/{gid}/members/$ref"

        headers = {
            "Authorization": f"Bearer {self.auth}",
            "Content-Type": "application/json",
        }

        data = {
            "@odata.id": f"https://graph.microsoft.com/v1.0/users/{member}"
        }
        res = requests.post(url, headers=headers, data=json.dumps(data))

    def guest_invite(self, email):
        url = "https://graph.microsoft.com/v1.0/invitations"
        headers = {
            "Authorization": f"Bearer {self.auth}",
            "Content-Type": "application/json",
        }

        data = {
            "invitedUserEmailAddress": email,
            "inviteRedirectUrl": "https://teams.microsoft.com/",
            "sendInvitationMessage": True,
        }

        res = requests.post(url, headers=headers, data=json.dumps(data))
        # data = json.loads(res.text)

    def delete_group(self, gid):
        url = f"https://graph.microsoft.com/v1.0/groups/{gid}"
        headers = {
            "Authorization": f"Bearer {self.auth}",
            "Content-Type": "application/json",
        }
        res = requests.delete(url, headers=headers)

    def get_users(self):
        url = "https://graph.microsoft.com/v1.0/users"
        headers = {
            "Authorization": f"Bearer {self.auth}",
            "Content-Type": "application/json",
        }
        params = {
            "$select": "id,mail",
        }
        res = requests.get(url, headers=headers, params=params)
        data = json.loads(res.text)
        link = data.get("@odata.nextLink")
        data = data.get("value")
        while link:
            res = requests.get(link, headers=headers)
            data2 = json.loads(res.text)
            link = data2.get("@odata.nextLink")
            data.extend(data2.get("value"))
        datadict = {}
        for x in data:
            if x.get("mail"):
                datadict[x.get("mail")] = x.get("id")
        return datadict

    def get_app_auth(self):
        url = f"https://login.microsoftonline.com/{dir_id}/oauth2/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": Applicationid,
            "client_secret": clientkey,
            "resource": "https://graph.microsoft.com",
        }

        res = requests.post(url, data)
        data = json.loads(res.text)
        return data.get('access_token')

    def get_me_auth(self):
        url = f"https://login.microsoftonline.com/{dir_id}/oauth2/token"
        data = {
            "grant_type": "password",
            "client_id": Applicationid,
            "client_secret": clientkey,
            "resource": "https://graph.microsoft.com",
            "username": config_data["SENDER_ID"],
            "password": config_data["SENDER_PASSWORD"]
        }

        res = requests.post(url, data)
        data = json.loads(res.text)
        return data.get('access_token')

    def create_group(self, name, class_id, members, owners):
        self.auth = self.get_app_auth()
        self.adusers = self.get_users()
        url = "https://graph.microsoft.com/v1.0/groups"
        headers = {
            "Authorization": f"Bearer {self.auth}",
            "Content-Type": "application/json",
        }
        members = [x[0] for x in members]
        owners = [x[0] for x in owners]
        owners.extend(self.owners.keys())
        mem = []
        onr = []

        for x in members:
            if self.adusers.get(x):
                mem.append(f"https://graph.microsoft.com/v1.0/users/{self.adusers.get(x)}")

        for x in owners:
            if self.adusers.get(x):
                if "@digiteams.ch" in str(x):
                    onr.append(f"https://graph.microsoft.com/v1.0/users/{self.adusers.get(x)}")
                else:
                    mem.append(f"https://graph.microsoft.com/v1.0/users/{self.adusers.get(x)}")
        mem = list(set(mem))
        onr = list(set(onr))
        for i in onr:
            if i in mem:
                mem.remove(i)

        data = {
            "displayName": name,
            "mailNickname": class_id,
            "description": f"class id is {class_id}",
            "visibility": "Private",
            "groupTypes": ["Unified"],
            "mailEnabled": True,
            "securityEnabled": False,
            "members@odata.bind": mem,
            "owners@odata.bind": onr
        }
        data = json.dumps(data)
        # # res = requests.post(url, data, headers=headers)
        res = requests.post(url, data, headers=headers)
        data = json.loads(res.text)
        if str(res.status_code) == "201":
            time.sleep(10)
            gid = data["id"]
            response = self.create_team(gid)
            return response, gid
        else:
            if data.get("error").get(
                    "message") == "Another object with the same value for property mailNickname already exists.":
                get_group_id = self.get_group_id(class_id)
                if get_group_id:
                    response = self.create_team(get_group_id)
                    return response, get_group_id
            else:
                err = data.get("error").get("message")
                print("Error creating Group:", err)
                logger.info(f"Error in creating Group :{err}")
                return False, False

    def create_team(self, group_id):
        url = f"https://graph.microsoft.com/v1.0/groups/{group_id}/team"
        headers = {
            "Authorization": f"Bearer {self.auth}",
            "Content-Type": "application/json",
        }
        data = {
            "memberSettings": {
                "allowCreateUpdateChannels": True
            },
            "messagingSettings": {
                "allowUserEditMessages": True,
                "allowUserDeleteMessages": True
            },
            "funSettings": {
                "allowGiphy": True,
                "giphyContentRating": "strict"
            }
        }

        data = json.dumps(data)
        res = requests.put(url, data, headers=headers)
        data = json.loads(res.text)
        if str(res.status_code) == "201":
            self.channel = data.get("internalId")
            print("Team created")
            logger.info(f"team Created.")
            return "success"
        elif str(res.status_code) == "502" and json.loads(res.text).get("error").get(
                "message") == "Failed to execute backend request.":
            self.channel = data.get("internalId")
            print("Team created")
            logger.info(f"Team Created")
            return "success"
        else:
            print("Team Creation failed", json.loads(res.text).get("error").get(
                "message"))
            logger.info(f"Team Creation Failed")
            self.channel = ""
            return "Fail"

    def online_meeting(self, date1, date2, owners, members, sub):
        meauth = self.get_me_auth()
        url = "https://graph.microsoft.com/beta/me/onlineMeetings"
        headers = {
            "Authorization": f"Bearer {meauth}",
            "Content-Type": "application/json",
        }
        offset = self.to_cet(date1)
        date1 = date1.split()
        date1 = f"{date1[0]}T{date1[1]}+{offset}"
        date2 = date2.split()
        date2 = f"{date2[0]}T{date2[1]}+{offset}"

        body = {
            "autoAdmittedUsers": "invitedUsersInCompany",
            "startDateTime": f"{date1.split('+')[0]}.8546353+{offset}",
            "endDateTime": f"{date2.split('+')[0]}.8546353+{offset}",
            "subject": sub,
            "chatInfo": {
                "threadId": self.channel
            },
        }
        res = requests.post(url, headers=headers, data=json.dumps(body))
        if str(res.status_code) == "201":
            data = json.loads(res.text)
            # uid = self.adusers.get(self.owners.keys()[0])
            res, dt = self.create_event(data.get("joinUrl"), date1, date2, members, owners, sub)
            if res == "success":
                return data.get("joinUrl"), dt
            else:
                print("Calender invite not sent")
                logger.info(f"Calender invite not sent.")
                return "fail", dt
        else:
            print("creation of online meeting failed")
            logger.info("creation of online meeting failed")

    def create_event(self, joinurl, date1, date2, members, owners, subject):
        u_id = self.adusers.get(config_data["SENDER_ID"])
        url = f"https://graph.microsoft.com/v1.0/users/{u_id}/calendar/events"
        headers = {
            "Authorization": f"Bearer {self.auth}",
            "Content-Type": "application/json",
            'Prefer': 'outlook.timezone="Central Europe Standard Time"'
        }
        users = []
        for m in members:
            users.append({
                "emailAddress": {
                    "address": m[0],
                    "name": m[1],
                },
                "type": "required"
            })
        for o in owners:
            users.append({
                "emailAddress": {
                    "address": o[0],
                    "name": o[1],
                },
                "type": "required"
            })

        otemp = [x[0] for x in owners]
        for k, v in self.owners.items():
            if k not in otemp:
                users.append({
                    "emailAddress": {
                        "address": k,
                        "name": v,
                    },
                    "type": "required"
                })

        data = {
            "subject": subject,
            "body": {
                "contentType": "HTML",
                "content": joinurl
            },
            "start": {
                "dateTime": date1,
                "timeZone": "Central Europe Standard Time"
            },
            "end": {
                "dateTime": date2,
                "timeZone": "Central Europe Standard Time"
            },
            "location": {
                "displayName": "Online"
            },
            "attendees": users
        }

        data = json.dumps(data)
        res = requests.post(url, data, headers=headers)
        # res = requests.post(url, data, headers=headers)
        if str(res.status_code) == "201":
            data = json.loads(res.text)
            print("Calender invite sent")
            logger.info(f"Calender invite sent. join url:{joinurl}")
            return "success", [[data.get("iCalUId")], [data.get("id")]]
        else:
            print("Calender invite not sent")
            logger.info(f"Calender invite not sent.")
            return "fail", ["", ""]

    def digi_class_data(self):
        self.dusers = self.get_digi_users()
        self.dclasses = self.get_digi_class()
        self.dorders = self.get_digi_orders()
        self.dterms = self.get_digi_terms()
        self.clsdata = {}
        for x in self.dclasses:
            if not self.clsdata.get(x["AUSSFId"]):
                self.clsdata[x["AUSSFId"]] = {"Trainer": [], "Students": []}
            for od in self.dorders:
                if x["AUSSFId"] == od["AUSSFId"]:
                    for usr in self.dusers:
                        if usr["TeilnehmerSFId"] == od["TeilnehmerSFId"]:
                            if od["ContactType"] == "Trainer":
                                self.clsdata[x["AUSSFId"]]["Trainer"].append(
                                    (usr["TeilnehmerEmail"], usr["TeilnehmerName"]))
                            else:
                                self.clsdata[x["AUSSFId"]]["Students"].append(
                                    (usr["TeilnehmerEmail"], usr["TeilnehmerName"]))
        return self.clsdata

    def get_digi_users(self):
        headers = {
            "Authorization": f"Bearer {config_data['DIGI_AUTH']}",
            "Content-Type": "application/json",
        }
        res = requests.get(config_data['USERS'], headers=headers)
        data = json.loads(res.text)
        return data

    def get_digi_class(self):
        headers = {
            "Authorization": f"Bearer {config_data['DIGI_AUTH']}",
            "Content-Type": "application/json",
        }
        res = requests.get(config_data['CLASS'], headers=headers)
        data = json.loads(res.text)
        return data

    def get_digi_orders(self):
        headers = {
            "Authorization": f"Bearer {config_data['DIGI_AUTH']}",
            "Content-Type": "application/json",
        }
        res = requests.get(config_data['ORDERS'], headers=headers)
        data = json.loads(res.text)
        return data

    def get_digi_terms(self):
        headers = {
            "Authorization": f"Bearer {config_data['DIGI_AUTH']}",
            "Content-Type": "application/json",
        }
        res = requests.get(config_data['TERMS'], headers=headers)
        data = json.loads(res.text)
        return data

    def get_ts(self):
        dt = datetime.datetime.now()
        date = dt.strftime("%Y-%m-%d %H:%M:%S")
        return str(date)

    def get_group_id(self, nickname):
        url = "https://graph.microsoft.com/v1.0/groups"
        headers = {
            "Authorization": f"Bearer {self.auth}",
            "Content-Type": "application/json",
        }

        res = requests.get(url, headers=headers)
        data = json.loads(res.text)
        for x in data.get("value"):
            if x.get("mailNickname") == nickname:
                return x.get("id")
        return False

    def get_teams(self, gid):
        url = f"https://graph.microsoft.com/v1.0/teams/{gid}"
        headers = {
            "Authorization": f"Bearer {self.auth}",
            "Content-Type": "application/json",
        }

        res = requests.get(url, headers=headers)
        data = json.loads(res.text)
        if str(res.status_code) == "200":
            self.channel = data.get("internalId")
        else:
            self.channel = ""

        return str(res.status_code)

    def get_group_members(self, gid):
        url1 = f"https://graph.microsoft.com/v1.0/groups/{gid}/owners"
        url2 = f"https://graph.microsoft.com/v1.0/groups/{gid}/members"
        headers = {
            "Authorization": f"Bearer {self.auth}",
            "Content-Type": "application/json",
        }
        res1 = requests.get(url1, headers=headers)
        res2 = requests.get(url2, headers=headers)
        data1 = json.loads(res1.text)
        data2 = json.loads(res2.text)
        members = []
        for x in data1.get("value"):
            members.append(x.get("mail"))
        for x in data2.get("value"):
            members.append(x.get("mail"))
        return set(members)

    def to_cet(self, date):
        cet = pytz.timezone('CET')
        dt = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
        offset = cet.utcoffset(dt)
        t = time.strftime("%H:%M", time.gmtime(offset.seconds))
        return t


def make_dir(*paths):
    for pt in paths:
        if not (os.path.isdir(pt)):
            try:
                os.makedirs(pt, mode=0o777, exist_ok=True)
            except OSError as exception:
                if exception.errno != errno.EEXIST:
                    raise


if __name__ == "__main__":
    LOG_PATH = os.path.join(os.getcwd(), "Logs")
    CONFIG_PATH = os.path.join(os.getcwd(), "config.json")
    config_data = json.load(open(CONFIG_PATH))
    make_dir(LOG_PATH)
    logging.basicConfig(filename=os.path.join(LOG_PATH, f"logfile_{datetime.datetime.now().strftime('%d-%m-%Y')}.log"),
                        format='%(asctime)s %(message)s',
                        filemode='a+')
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    db_host = "localhost"
    db_name = "YOUR_MYSQL_DATABASE_NAME"
    db_user = "YOUR_MYSQL_USERNAME"
    db_password = "YOUR_MYSQL_PASSWORD"

    warnings.simplefilter("ignore")
    dir_id = config_data["DIRECTORY_ID"]
    Applicationid = config_data["APPLICATION_ID"]
    clientkey = config_data["CLIENT_KEY"]
    MicrosoftTeams()