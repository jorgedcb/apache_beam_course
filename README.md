# apache_beam_course
In this repository I will upload the code for my apache beam course

I used python 3.8 so:
py -3.8 -m venv venv
venv\Scripts\activate
pip install -r requirements.txt

to run the code:
python main.py --input harrypotter\1_The_Philosopher's_Stone.txt  --output output\ --runner DirectRunner 

to run the unitest:
python -m pytest -v