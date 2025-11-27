import os
from dotenv import load_dotenv

load_dotenv()
MODE=os.getenv('MODE')
WHATSAPP_PRUEBAS=os.getenv("WHATSAPP_PRUEBAS_NUMBER")
TIMEZONE=os.getenv("TIMEZONE")
SECRET_KEY=os.getenv("JWT_SECRET")
JWT_TTL=os.getenv("JWT_TTL")
EMAIL_SENDER=os.getenv("MAIL_USERNAME")
EMAIL_SENDER_PASSWORD=os.getenv("MAIL_PASSWORD")
EMAIL_CITAS=os.getenv("MAIL_CITAS")
EMAIL_SISTEMAS=os.getenv("MAIL_SISTEMAS")