
'''
yearRate is a function that takes the list of the word, and returns a list with the pair of years as well as their
change in both by an absolute amount and in procent from previous years.
'''


def yearRate(word):
    wordLength = len(word)
    wordArray=[]
    for i in range(0,wordLength-2):
        diff = int(word[i][1])-int(word[i+1][2])
        diffProcent = float(word[i][2])/float(word[i+1][2])*100
        yearPair = word[i][0]+"-"+word[i+1][0]
        wordArray.append([yearPair,diff,diffProcent])
    return wordArray

'''
With the buzzWord function we can set a threshold regarding the set of words of each word pair , however a big threshold
is recommended. 
'''


def buzzWord(list,threshold):
    listLength = len(list)
    buzzList=[]
    for i in range(0, listLength - 1):
        if list[i][2] >= threshold:
            buzzList.append(list[i])
    return buzzList


def main():
    print("Output in the form of Year-pair, increase in usage by numbers, and increase in procent")
    print('[year pair,change in usage[real number],change in usage[procent]]')
    exampleWord = [['1797', '1', '1', '1'], ['1845', '2', '2', '2'], ['1869', '1', '1', '1'], ['1885', '1', '1', '1'],
                   ['1887', '3', '3', '3'], ['1889', '1', '1', '1'], ['1900', '1', '1', '1'], ['1928', '1', '1', '1'],
                   ['1977', '2', '2', '2'], ['1978', '2', '2', '2'], ['1979', '1', '1', '1'], ['1980', '10', '10', '3'],
                   ['1981', '3', '3', '3'], ['1987', '7', '5', '2'], ['1988', '19', '15', '5'],
                   ['1991', '29', '9', '3'], ['1994', '4', '4', '1'], ['1997', '2', '2', '1'], ['1999', '2', '1', '1'],
                   ['2001', '7', '6', '6'], ['2002', '3', '2', '1'], ['2003', '13', '10', '4'],
                   ['2005', '45', '15', '2']]
    yearPair = yearRate(exampleWord)
    threshold = 100
    thresPairs = buzzWord(yearPair,threshold)
    print(yearPair)
    print(thresPairs)

main()