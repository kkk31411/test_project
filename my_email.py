import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# -------------------------
# 네이버 SMTP 계정 정보
# -------------------------
SMTP_SERVER = "smtp.naver.com"
SMTP_PORT = 587
EMAIL = "shk6650@naver.com"       # 발신자 이메일
PASSWORD = "PY6JMZXDS3Y4"          # 앱 비밀번호

def send_email(to_email, subject, body):
    """이메일 발송 함수"""
    try:
        msg = MIMEText(body, "plain", "utf-8")
        msg["From"] = EMAIL
        msg["To"] = to_email
        msg["Subject"] = subject

        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(EMAIL, PASSWORD)
        server.send_message(msg)
        server.quit()
        print(f"✅ 이메일 전송 성공: {to_email}")
        return True
    except Exception as e:
        print("❌ 이메일 전송 실패:", e)
        return False
    
