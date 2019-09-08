package it.unibo.bd1819.scoreanswersbins;

public enum Bin {
    HIGH_SCORE_HIGH_COUNT,
    HIGH_SCORE_LOW_COUNT,
    LOW_SCORE_HIGH_COUNT,
    LOW_SCORE_LOW_COUNT;

    public static final int DEFAULT_SCORE_THRESHOLD = 10;
    public static final int DEFAULT_ANSWERS_COUNT_THRESHOLD = 5;
    private static final String BASE_CONF_PATH = "scoreanswersbins";
    public static final String SCORE_THRESHOLD_CONF = BASE_CONF_PATH + ".score.threshold";
    public static final String ANSWERS_COUNT_THRESHOLD_CONF = BASE_CONF_PATH + ".answerscount.threshold";

    public static Bin getBinFor(final int score, final int scoreThreshold, final int count, final int countThreshold) {
        if (score > scoreThreshold) {
            if (count > countThreshold) {
                return HIGH_SCORE_HIGH_COUNT;
            } else {
                return HIGH_SCORE_LOW_COUNT;
            }
        } else {
            if (count > countThreshold) {
                return LOW_SCORE_HIGH_COUNT;
            } else {
                return LOW_SCORE_LOW_COUNT;
            }
        }
    }
}
