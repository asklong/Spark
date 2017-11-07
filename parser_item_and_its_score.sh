#!/usr/bin/env bash
function parser_item_and_its_score {
    INPUT_PATH=$1
    SAVE_PATH=$2

    hadoop fs -test -e $SAVE_PATH
    if [ $? -eq 0 ];then
         hadoop fs -rmr ${SAVE_PATH}
    fi

spark-submit \
    --master yarn-client \
    --queue root.bdp_jmart_ad.jd_ad_retr \
    --conf spark.akka.frameSize=150 \
    --conf spark.core.connection.ack.wait.timeout=6000 \
    --conf spark.rdd.compress=true \
    --conf spark.storage.memoryFraction=0.6 \
    --conf spark.driver.maxResultSize=20g \
    --num-executors 100 \
    --driver-memory 16g \
    --executor-memory 16g \
    --executor-cores 8 \
    parser_item_and_its_score.py ${INPUT_PATH} ${SAVE_PATH}
}

#c_gen_cross_cid_pair "/user/jd_ad/ads_reco/yan.yan/CrossCIDRetrievalFormatted_version1"  "/user/jd_ad/ads_reco/kejin/clickrate/cross_cid/"

#c_gen_cross_cid_pair "/user/jd_ad/ads_reco/yan.yan/CID2RetrievalFormatted"  "/user/jd_ad/ads_reco/kejin/clickrate/cross_cid2/"

#c_gen_cross_cid_pair "/user/jd_ad/ads_reco/yan.yan/CID1RetrievalFormatted"  "/user/jd_ad/ads_reco/kejin/clickrate/cross_cid3/"

#/user/jd_ad/ads_reco/kejin/clickrate/detail_like

#/user/jd_ad/ads_reco/kejin/clickrate/cause_items/fp_mid/

#c_gen_cross_cid_pair "/user/jd_ad/ads_reco/kejin/clickrate/word2vec"  "/user/jd_ad/ads_reco/kejin/clickrate/cause_items/word2vec/"
c_gen_cross_cid_pair "/user/jd_ad/ads_reco/kejin/clickrate/fp_mid" "/user/jd_ad/ads_reco/kejin/clickrate/cause_items/fp_mid/"
