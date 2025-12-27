FROM python:3.12-trixie

ARG USER=serviceuser

ENV USER=$USER
ENV HOME=/home/$USER

RUN useradd --create-home --shell /bin/bash $USER && \
    chown -R $USER:$USER $HOME && \
    usermod -aG dialout $USER
USER $USER
WORKDIR $HOME/app
ADD --chown=$USER:$USER . .

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "main.py"]