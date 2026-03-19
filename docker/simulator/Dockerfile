FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    faker==20.1.0 \
    psycopg2-binary==2.9.9

# Copy the simulator code
COPY src/simulator/transaction_simulator.py /app/

# Run the simulator
CMD ["python", "transaction_simulator.py", "--tps", "2", "--fraud", "10"]