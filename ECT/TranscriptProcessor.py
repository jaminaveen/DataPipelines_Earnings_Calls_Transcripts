
from bs4 import BeautifulSoup


def get_mappings(soup):
    mappings = []
    heading_on = False
    all_text = soup.find_all('p')
    heads_counter = 0
    heads_list = soup.find_all('strong')
    try:
        for i in range(len(all_text)):
            heads = all_text[i].find_all('strong')
            if len(heads)>0 and not heading_on:
                pair_id =  heads_list[heads_counter].text
                for each in all_text[i].find_all_next("p"):
                    #pair_id =  each.text
                    if len(each.find_all('strong')) == 0:
                        #print(pair_id+"-"+each.text)
                        mappings.append((pair_id,each.text))
                    else:
                        heads_counter = heads_counter+1
                        ##print(each)
                        if len(each.find_all('span'))>0 and each.find_all('span')[0]['class'][0]=='question':
                            #print('question')
                            mappings.append((heads_list[heads_counter].text,'question',each.text))
                        if len(each.find_all('span'))>0 and each.find_all('span')[0]['class'][0]=='answer':
                            #print('answer')
                            mappings.append((heads_list[heads_counter].text,'answer',each.text))
                        break
        processed_intro_mappings = []
        q_and_a = False
        for each in range(len(mappings)):
            if len(mappings[each])==2 and not q_and_a:
                q_and_a = False
                processed_intro_mappings.append(mappings[each])
            if len(mappings[each])==3:
                q_and_a = True
                break
        processed_qa_mappings = []
        q_and_a = False
        for each in range(len(mappings)):
            if len(mappings[each])==2 and q_and_a:
                q_and_a = False
                #processed_qa_mappings.append(mappings[each])
            if len(mappings[each])==3:
                q_and_a = True
                processed_qa_mappings.append((mappings[each][0],mappings[each][1],mappings[each+1][1]))
                continue
        processed_conclusion_mappings = []
        q_and_a_start = False
        q_and_a_over =  False
        for each in reversed(range(len(mappings))):
            if len(mappings[each])==2 and not q_and_a_over:
                q_and_a_over = False
                processed_conclusion_mappings.append(mappings[each])
            if len(mappings[each])==3:
                q_and_a_over = True
                break
        processed_conclusion_mappings.reverse()
    except Exception as e:
        raise e
    
    return [processed_intro_mappings, processed_qa_mappings, processed_conclusion_mappings[1:]]
    


