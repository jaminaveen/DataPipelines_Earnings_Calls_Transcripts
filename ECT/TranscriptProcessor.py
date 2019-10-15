

def get_mappings(soup):
    """

    :param soup: beautiful soup object of the html strings stored in "raw" collection mongodb
    :return: list of 3 lists -> [processed_intro_mappings, processed_qa_mappings, processed_conclusion_mappings]
    processed_intro_mappings -> list of tuples(speaker,message)
    processed_qa_mappings -> list of tuples (speaker,type(question or answer), message)
    processed_conclusion_mappings -> list of tuples (speaker,message)
    """
    mappings = []
    heading_on = False
    all_text = soup.find_all('p')  # list of all paragraphs
    heads_counter = 0  # a counter to track the required heading
    heads_list = soup.find_all('strong')  # list of all headings in the transcript
    try:
        """this loop will map all the headings, the corresponding 
        text/information in a transcript and store them as list of tuples"""
        for i in range(len(all_text)):
            heads = all_text[i].find_all('strong')
            if len(heads) > 0 and not heading_on:
                pair_id = heads_list[heads_counter].text   # find header and loop to get all the related paragaphs by looping over the next "p" elements
                for each in all_text[i].find_all_next("p"):
                    # map (speaker,text) which are not part Q and A section i.e, Intro and Conclusion sections
                    if len(each.find_all('strong')) == 0:
                        mappings.append((pair_id,each.text))
                    # map (speaker, message_type(Q or A), text)
                    else:
                        heads_counter = heads_counter+1

                        if len(each.find_all('span')) > 0 and each.find_all('span')[0]['class'][0] == 'question':
                            mappings.append((heads_list[heads_counter].text,'question',each.text))
                        if len(each.find_all('span')) > 0 and each.find_all('span')[0]['class'][0] == 'answer':
                            mappings.append((heads_list[heads_counter].text,'answer',each.text))
                        break

        """code to segregate mappings into Intro section, Q/A section and Conclusion Section"""
        processed_intro_mappings = []
        q_and_a = False
        """iterate through all mappings till Q/A section begins and result will be Intro Section"""
        for each in range(len(mappings)):
            if len(mappings[each]) == 2 and not q_and_a:
                q_and_a = False
                processed_intro_mappings.append(mappings[each]) # map intro section
            if len(mappings[each]) == 3:
                q_and_a = True
                break  # end of intro section mappings once we observe Q/A begins
        processed_qa_mappings = []
        q_and_a = False
        for each in range(len(mappings)):
            if len(mappings[each]) == 2 and q_and_a:
                q_and_a = False  # skip intro section
            if len(mappings[each]) == 3: # verify if mapping is part of Q/A
                q_and_a = True
                processed_qa_mappings.append((mappings[each][0],mappings[each][1],mappings[each+1][1])) # append to Q/A section
                continue
        processed_conclusion_mappings = []
        q_and_a_over = False
        # iterate from the end using reverse() and map conclusion statements till we encounter Q/A mappings
        for each in reversed(range(len(mappings))):
            if len(mappings[each]) == 2 and not q_and_a_over:
                q_and_a_over = False
                processed_conclusion_mappings.append(mappings[each])
            if len(mappings[each]) == 3:
                q_and_a_over = True
                break
        processed_conclusion_mappings.reverse()  # reverse the conclusions mapped to get the sorted conclusion mappings
    except Exception as e:
        raise e
    
    return [processed_intro_mappings, processed_qa_mappings, processed_conclusion_mappings[1:]]
    


