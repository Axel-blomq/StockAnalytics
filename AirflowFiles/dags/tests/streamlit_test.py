import requests

def test_streamlit(url="http://localhost:8501"):
    try:
        r = requests.get(url, timeout=5)

        if r.status_code == 200:
            print("Streamlit server reachable")
            return True
        else:
            print(f"Streamlit responded but status code {r.status_code}")
            return False

    except Exception as e:
        print(f"Streamlit connection failed: {e}")
        return False


if __name__ == "__main__":
    test_streamlit()