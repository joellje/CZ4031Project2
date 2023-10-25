# To start database

-   Clone repository:

```
git clone https://github.com/joellje/CZ4031Project2.git
```

-   Copy csv files into ./db/data/
-   Run shell script to start DB:

```
cd scripts
./init_db.sh
```

# To run GUI

-   Create a new Python virtual environment:

```
python -m venv venv (Mac)

py -m venv venv (Windows 11)
```

-   Start virtual environment manually by running:

```
source venv/bin/activate (Mac)

.\venv\Scripts\activate (Windows 11)
```

-   Install Python requirements in the project repository: `pip install -r requirements.txt`

-   Run GUI: `python project.py`
