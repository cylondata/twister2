javas=""
space=" "
for f in $(find ../../ -name '*.java'); do javas=$f$space$javas; done

rm static/javadocs -f -r
mkdir static/javadocs
javadoc $javas -d static/javadocs -exclude com.twitter.bazel.checkstyle -notimestamp
