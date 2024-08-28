FROM amazon/aws-glue-libs:glue_libs_4.0.0_image_01

USER root

RUN mkdir -p /home/glue_user/app/
	  
COPY run_competition.py /home/glue_user/app/

VOLUME ["./spark-conf:/home/glue_user/spark/conf"]

CMD ["spark-submit"]
