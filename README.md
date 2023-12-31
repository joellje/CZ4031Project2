# To start database

- Clone repository:

```
git clone https://github.com/joellje/CZ4031Project2.git
```

- Copy TPCH csv files into ./db/data/
- Install Docker from https://docs.docker.com/engine/install/
- Run shell script to start DB:

```
cd scripts
./init_db.sh
```

# To run GUI

- Create a new Python virtual environment:

```
python -m venv venv (Mac)

py -m venv venv (Windows 11)
```

- Start virtual environment manually by running:

```
source venv/bin/activate (Mac)

.\venv\Scripts\activate (Windows 11)
```

- Install Python requirements in the project repository: `pip install -r requirements.txt`

- Ensure you are connected to Internet, because the visualisation of the QEP tree requires connection to an external PyPlot JavaScript file hosted on a CDN.

- Run GUI: `python project.py` (Make sure that you minimally have Python 3.10 installed.)
