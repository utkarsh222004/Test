#!/usr/bin/env python3
import sys

def char_mapper():
    for line in sys.stdin:
        line = line.strip()
        for char in line:
            if char != ' ':
                print(f'{char}\t1')

def char_reducer():
    current_char = None
    count = 0

    for line in sys.stdin:
        char, value = line.strip().split('\t')
        value = int(value)

        if current_char == char:
            count += value
        else:
            if current_char:
                print(f'{current_char}\t{count}')
            current_char = char
            count = value

    if current_char == char:
        print(f'{current_char}\t{count}')

def word_mapper():
    for line in sys.stdin:
        line = line.strip()
        for word in line.split():
            print(f'{word.lower()}\t1')

def word_reducer():
    current_word = None
    count = 0

    for line in sys.stdin:
        word, value = line.strip().split('\t')
        value = int(value)

        if word == current_word:
            count += value
        else:
            if current_word:
                print(f'{current_word}\t{count}')
            current_word = word
            count = value

    if current_word == word:
        print(f'{current_word}\t{count}')


if __name__ == "__main__":
    if "--mapper" in sys.argv:
        # Switch between char_mapper or word_mapper here
        char_mapper()  # ← For character count
        # word_mapper()  # ← Uncomment this instead for word count
    elif "--reducer" in sys.argv:
        # Switch between char_reducer or word_reducer here
        char_reducer()  # ← For character count
        # word_reducer()  # ← Uncomment this instead for word count





#--------------Commands-------------------

# Char count

# hadoop jar C:\hadoop\share\hadoop\tools\lib\hadoop-streaming-3.2.4.jar ^
# -files ci3_word_mapreduce.py ^
# -mapper "python ci3_word_mapreduce.py --mapper" ^
# -reducer "python ci3_word_mapreduce.py --reducer" ^
# -input file:///C:\Users\Saiashish\Desktop\CL3_lab_practicals\input.txt ^
# -output file:///C:\Users\Saiashish\Desktop\CL3_lab_practicals\char_output

# type C:\Users\Saiashish\Desktop\CL3_lab_practicals\char_output\part-00000




# Word Count

# hadoop jar C:\hadoop\share\hadoop\tools\lib\hadoop-streaming-3.2.4.jar ^
# -files ci3_word_mapreduce.py ^
# -mapper "python ci3_word_mapreduce.py --mapper" ^
# -reducer "python ci3_word_mapreduce.py --reducer" ^
# -input file:///C:\Users\Saiashish\Desktop\CL3_lab_practicals\input.txt ^
# -output file:///C:\Users\Saiashish\Desktop\CL3_lab_practicals\word_output

# type C:\Users\Saiashish\Desktop\CL3_lab_practicals\word_output\part-00000


