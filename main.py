import psycopg2


def drop_table(connection, cursor):
    cursor.execute('''
    DROP TABLE phones;
    DROP TABLE clients;
    ''')


def create_table(connection, cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS clients(
        id SERIAL PRIMARY KEY,
        name VARCHAR(40) NOT NULL,
        lastname VARCHAR(40) NOT NULL,
        email VARCHAR(40) UNIQUE NOT NULL
    );
        CREATE TABLE IF NOT EXISTS phones(
        id SERIAL PRIMARY KEY,
        phone VARCHAR(20) UNIQUE,
        clients_id INTEGER NOT NULL REFERENCES clients(id)
    );
    ''')


def create_new_client(connection, cursor, name, lastname, email, phone=None):
    cursor.execute('''INSERT INTO clients(name, lastname, email) VALUES(%s, %s, %s); 
    INSERT INTO phones(phone, clients_id) VALUES(%s, (SELECT id FROM clients ORDER BY id DESC LIMIT 1));''',
                   (name, lastname, email, phone))


def add_phone(connection, cursor, phone, id):
    cursor.execute('''INSERT INTO phones(phone, clients_id) VALUES(%s, %s);''', (phone, id))


def data_change(connection, cursor, id, key, value):
    i = {'name': 'name', 'lastname': 'lastname', 'email': 'email', 'phone': 'phone'}
    if i.get(key) == 'phone':
        sql = 'UPDATE phones SET phone = %s WHERE id = %s'
        cursor.execute(sql, [value, id])
    else:
        sql = f'UPDATE clients SET {i.get(key)} = %s WHERE id = %s'
        cursor.execute(sql, [value, id])


def delete_phone(connection, cursor, id):
    cursor.execute('''DELETE FROM phones WHERE id = %s;''', (id,))


def delete_client(connection, cursor, id):
    cursor.execute('''DELETE FROM phones WHERE clients_id = %s;
    DELETE FROM clients WHERE id = %s''', (id, id))


def find_client(connection, cursor, val):
    cursor.execute('''SELECT * FROM clients, phones 
    WHERE to_tsvector(name || ' ' || lastname || ' ' || email || ' ' || (to_tsvector(coalesce(phone ,'')), 'alter_NULL')) @@ to_tsquery(%s)
    ORDER BY name DESC;''', (val,))
    print(cursor.fetchall())


if __name__ == "__main__":
    with psycopg2.connect(database='climanage_db', user='postgres', password='12345678', host='127.0.0.1',
                          port='5432') as connection:
        connection.autocommit = True
        with connection.cursor() as cursor:
            # drop_table(connection, cursor)
            create_table(connection, cursor)
            # create_new_client(connection, cursor, 'Ivan', 'Ivanov', 'ivanov@gmail.com', '1234567890')
            # create_new_client(connection, cursor, 'Peter', 'Petrov', 'petrov@gmail.com', '0987654321')
            # add_phone(connection, cursor, '666', '1')
            # data_change(connection, cursor, '1', 'name', 'Bruce')
            # data_change(connection, cursor, '2', 'phone', '9999999999999')
            # delete_phone(connection, cursor, '3')
            # delete_client(connection, cursor, '1')
            # find_client(connection, cursor, 'Ivan')
    connection.close()
