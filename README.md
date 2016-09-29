# spake-ml sample program

이 프로그램은 spark-ml을 학습하기 위해 작성한 샘플 코드입니다.

# 포함된 내용 
* word2vec
* kmean
* spark mongodb connector


# 주의사항
* 최소 8g 이상 힙메모리 설정해야 함(-Xmx8g 설정 필요)

# 사용법
1. https://github.com/focuschange/test-colletion 에서 terms.txt 파일을 받은 후 $PROJECT_HOME/data 디렉토리에 넣어야 함
2. Word2VecTest.run 을 통해서 word2vec model 생성
1. KMeanExampleTest.run을 통해서 kmean model 생성 
1. "run.sh data-dir" 을 사용하여 데이터 검증  

## 참고
* 별도 라이센스 없습니다. 
* 당연히 소스 퀄리티는 보장하지 않습니다

