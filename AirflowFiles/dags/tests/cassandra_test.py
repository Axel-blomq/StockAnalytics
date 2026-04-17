from cassandra.cluster import Cluster

def test_cassandra():
    try:
        cluster = Cluster(['127.0.0.1'])
        session = cluster.connect('stocks')

        rows = session.execute("SELECT release_version FROM system.local")

        for row in rows:
            print("Cassandra version:", row.release_version)

        session.shutdown()
        cluster.shutdown()

        print("Cassandra connection successful")
        return True

    except Exception as e:
        print("Cassandra connection failed:", e)
        return False


if __name__ == "__main__":
    test_cassandra()