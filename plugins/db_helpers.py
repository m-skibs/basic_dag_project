import sqlite3
from typing import List


def create_table(db_name) -> None:
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS users
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 name TEXT,
                 email TEXT,
                 signup_date DATE,
                 interests TEXT,
                 email_is_valid BOOLEAN)''')
    conn.commit()
    conn.close()


def store_data(self, data) -> None:
    conn = sqlite3.connect(self.db_file)
    c = conn.cursor()
    for record in data:
        c.execute("INSERT INTO users (name, email, signup_date, interests) VALUES (?, ?, ?, ?)",
                  (record['name'], record['email'], record['signup_date'], ','.join(record['interests'])))
    conn.commit()
    conn.close()


def update_table(db_name, table_name, table_data) -> None:
    conn = sqlite3.connect(db_name)
    table_data.to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()


def update_record(db_name, user_id, new_interests) -> None:
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute("UPDATE users SET interests=? WHERE id=?",
              (new_interests, user_id))
    conn.commit()
    conn.close()


def delete_record(db_name, user_id) -> None:
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute("DELETE FROM users WHERE id=?", (user_id,))
    conn.commit()
    conn.close()


def merge_duplicates(db_name) -> None:
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute("SELECT email FROM users GROUP BY email HAVING COUNT(*) > 1")
    duplicate_emails = [row[0] for row in c.fetchall()]
    for email in duplicate_emails:
        c.execute("SELECT * FROM users WHERE email=?", (email,))
        duplicates = c.fetchall()
        id_to_keep = duplicates[0][0]
        all_interests = [duplicates[0][4]]
        to_del = []
        for dupe in duplicates[1:]:
            dupe_id = dupe[0]
            dupe_interests = dupe[4]
            all_interests.append(dupe_interests)
            to_del.append(dupe_id)


        a = ','.join(all_interests)
        a = a.replace('"', '')
        a = a.split(',')
        a = ','.join(list(set(a)))

        update_record(db_name, id_to_keep, a)
        for td in to_del:
            delete_record(db_name, td)

    conn.close()


def fetch_users_by_interest_and_signup_date(db_name, interest) -> List:
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE signup_date >= DATE('now', '-1 month') "
              "AND interests LIKE '%'||?||'%' ORDER BY signup_date", (interest,))
    users = c.fetchall()
    conn.close()
    return users
