import qrcode

# Data to be encoded in the QR code
data = "https://www.example.com"

# Create a QR code instance
qr = qrcode.QRCode(
    version=1,  # Controls the size and data capacity of the QR code (1-40)
    error_correction=qrcode.constants.ERROR_CORRECT_L, # Error correction level (L, M, Q, H)
    box_size=10,  # Size of each box (pixel) in the QR code
    border=4,  # Thickness of the border around the QR code
)

# Add data to the QR code
qr.add_data(data)
qr.make(fit=True)

# Create an image from the QR code instance
img = qr.make_image(fill_color="black", back_color="white")

# Save the QR code image
img.save("my_qr_code.png")

print("QR code generated and saved as my_qr_code.png")