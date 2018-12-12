#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  5 09:48:07 2018

"""
import nltk
#dato un testo, ritorna una lista di triple <Feature, Modifier, Sentiment>, 
#corrispondenti a <Sostantivo, Avverbio, Aggettivo>

def getTriples(review):

        #tokenizza
        text = nltk.word_tokenize(review)

        grammar = """NP: 
     
                 
                  
                 {<RB>?<JJ><NN>}   
                 {<RB>?<JJ>+<NNS>}
                
                
              
    
                 {<NN><JJ>+} 
                 {<RB><NN>}
                
  
                 {<NN><VBD><RB>?<JJ><CC><JJ>}    
                 {<NNS><VBD><RB>?<JJ><CC><JJ>}
                 {<NN><VBD><RB>?<JJ><CC><JJ>}
                
                 
                 {<NN><VBZ><RB>?<JJ><CC><JJ>}    
                 {<NNS><VBZ><RB>?<JJ><CC><JJ>}
                 {<NN><VBZ><RB>?<JJ><CC><JJ>}
                 
                 {<NN><RB>?<RB>?<JJ>}
                 
                 {<NN><VBZ><RB>?<RB>?<JJ>}
                 {<NN><VBD><RB>?<RB>?<JJ>}
                 {<NNS><VBZ><RB>?<RB>?<JJ>}
                 {<NNS><VBD><RB>?<RB>?<JJ>}
                 {<RB>?<RB>?<JJ>+<NN><CC><NN>}
                 {<RB>?<RB>?<JJ><CC><JJ><NN>}
                        
                
                
                 
                 
                 
                 
                 {<NNS><VBD><RB>?<VBG>}
                 {<NNS><CC><NN><VBD><RB>?<VBG>}
                 {<NN><CC><NNS><VBD><RB>?<VBG>}
                 {<NNS><CC><NNS><VBD><RB>?<VBG>}
                  
                 {<NN><VBD><RB>?<VBG>}    
                 {<NNS><VBD><RB>?<VBG>}
                 {<NN><VBD><RB>?<VBG>}
                 
                 {<NN><VBZ><RB>?<VBG>}    
                 {<NNS><VBZ><RB>?<VBG>}
                 {<NN><VBZ><RB>?<VBG>}
                             
                 {<NNS><VBP><RB>?<VBG>}
                 {<NNS><CC><NN><VBP><RB>?<VBG>}
                 {<NN><CC><NNS><VBP><RB>?<VBG>}
                 {<NNS><CC><NNS><VBP><RB>?<VBG>}
                 
                 
                 {<PRP><VBD><RB>?<RB><JJ><NN><CC><NN>}
                 {<PRP><VBD><RB>?<RB><JJ><NNS><CC><NN>}
                 {<PRP><VBD><RB>?<RB><JJ><NN><CC><NNS>}
                 {<PRP><VBD><RB>?<RB><JJ><NNS><CC><NNS>}
                 {<PRP><VBD><RB>?<RB><KJ><NN>}
                 {<PRP><VBD><RB>?<RB><JJ><NNS>}
                 

                 {<PRP><VBD><RB>?<RB><VB><NN><CC><NN>}
                 {<PRP><VBD><RB>?<RB><VB><NNS><CC><NN>}
                 {<PRP><VBD><RB>?<RB><VB><NN><CC><NNS>}
                 {<PRP><VBD><RB>?<RB><VB><NNS><CC><NNS>}
                 {<PRP><VBD><RB>?<RB><VB><NN>}
                 {<PRP><VBD><RB>?<RB><VB><NNS>}

                     
                 
                 {<PRP><VBP><RB><VB><NN>}
              
                 
 
                 {<PRP><VBP><RB>?<RB><VBN><NN><CC><NN>}
                 {<PRP><VBP><RB>?<RB><VBN><NNS><CC><NN>}
                 {<PRP><VBP><RB>?<RB><VBN><NN><CC><NNS>}
                 {<PRP><VBP><RB>?<RB><VBN><NNS><CC><NNS>}
                 {<PRP><VBP><RB>?<RB><VBN><NN>}
                 {<PRP><VBP><RB>?<RB><VBN><NNS>}
                 

                 {<PRP><VBD><RB>?<RB><VBN><NN><CC><NN>}
                 {<PRP><VBD><RB>?<RB><VBN><NN><CC><NNS>}
                 {<PRP><VBD><RB>?<RB><VBN><NNS><CC><NN>}
                 {<PRP><VBD><RB>?<RB><VBN><NNS><CC><NNS>} 
                 {<PRP><VBD><RB>?<RB><VBN><NN>}
                 {<PRP><VBD><RB>?<RB><VBN><NNS>}
                 
                 
                 {<PRP><VBD><RB>?<RB><IN><NN><CC><NN>}
                 {<PRP><VBD><RB>?<RB><IN><NN><CC><NNS>}
                 {<PRP><VBD><RB>?<RB><IN><NNS><CC><NN>}
                 {<PRP><VBD><RB>?<RB><IN><NNS><CC><NNS>} 
                 {<PRP><VBD><RB>?<RB><IN><NN>}
                 {<PRP><VBD><RB>?<RB><IN><NNS>}
                 
                 
                  {<PRP><VBP><RB>?<RB><IN><NN><CC><NN>}
                 {<PRP><VBP><RB>?<RB><IN><NN><CC><NNS>}
                 {<PRP><VBP><RB>?<RB><IN><NNS><CC><NN>}
                 {<PRP><VBP><RB>?<RB><IN><NNS><CC><NNS>} 
                 {<PRP><VBP><RB>?<RB><IN><NN>}
                 {<PRP><VBP><RB>?<RB><IN><NNS>}
                 
           
                 
                 {<PRP><RB>?<VBD><NN><CC><NN>}
                 {<PRP><RB>?<VBD><NNS><CC><NN>}
                 {<PRP><RB>?<VBD><NN><CC><NNS>}
                 {<PRP><RB>?<VBD><NNS><CC><NNS>}                 
                 {<PRP><RB>?<VBD><NN>}
                 {<PRP><RB>?<VBD><NNS>}
                 {<PRP><RB>?<VB><NN>}
                 {<PRP><RB>?<VB><NNS>}
                 
                 
                 {<PRP><RB>?<VBP><NN><CC><NN>}
                 {<PRP><RB>?<VBP><NNS><CC><NN>}
                 {<PRP><RB>?<VBP><NN><CC><NNS>}
                 {<PRP><RB>?<VBP><NNS><CC><NNS>}                 
                 {<PRP><RB>?<VBP><NN>}
                 {<PRP><RB>?<VBP><NNS>}
                 
                 
                 {<PRP><RB>?<IN><NN><CC><NN>}
                 {<PRP><RB>?<IN><NNS><CC><NN>}
                 {<PRP><RB>?<IN><NN><CC><NNS>}
                 {<PRP><RB>?<IN><NNS><CC><NNS>}                
                 {<PRP><RB>?<IN><NN>}
                 {<PRP><RB>?<IN><NNS>}
                 
                 
            
             
                 {<PRP><RB>?<VBP><NN><CC>}
                 {<PRP><RB><IN><NN><CC>}
                
            
            
                 {<NN><CC>?<NN><VBZ><RB>?<RB>?<JJ>+}      
                 {<NNS><CC>?<NN><VBZ><RB>?<RB>?<JJ>+}      
                 {<NN><CC>?<NNS><VBZ><RB>?<RB>?<JJ>+} 
                 {<NNS><CC>?<NNS><VBZ><RB>?<RB>?<JJ>+}
                 
                 
                 
                 {<NN><CC>?<NN><VBD><RB>?<RB>?<JJ>+}      
                 {<NNS><CC>?<NN><VBD><RB>?<RB>?<JJ>+}      
                 {<NN><CC>?<NNS><VBD><RB>?<RB>?<JJ>+} 
                 {<NNS><CC>?<NNS><VBD><RB>?<RB>?<JJ>+} 
                 
                 
                 
                 {<NN><CC>?<NN><VBP><RB>?<RB>?<JJ>+}      
                 {<NNS><CC>?<NN><VBP><RB>?<RB>?<JJ>+}      
                 {<NN><CC>?<NNS><VBP><RB>?<RB>?<JJ>+} 
                 {<NNS><CC>?<NNS><VBP><RB>?<RB>?<JJ>+} 
 
                 
                 {<NN><VBD><RB>?<JJ>?}    
                 {<NNS><VBD><RB>?<JJ>+}
                 {<NN><VBD><RB>?<JJ>+}
                
                 
                 {<NN><VBZ><RB>?<JJ>?}    
                 {<NNS><VBZ><RB>?<JJ>+}
                 {<NN><VBZ><RB>?<JJ>+}
                
                  {<RB><VBN><NN>}
                  {<NN><RB><JJ>}
                  {<NNS><RB><JJ>}
           """
        
        #parse tree                
        parser = nltk.RegexpParser(grammar)
        t = parser.parse(nltk.pos_tag(text))
        
        results = []
              
        #select only the NP from the tree
        for s in t.subtrees():
            if s.label() == "NP":
                results.append(s.leaves())

      
        feature = []  
        modifier = []
        sentiment = []
        
        triple = []
        for element in results:
            for into in element:
                    #se è un nome singolare o plurale allora è una feature
                if into[1] == "NN" or into[1] == "NNS":
                    feature.append(into[0] ) 
                    #se è un aggettivo, o ci sono le parole 'love' e 'like', è un sentiment
                if into[1] == "JJ" or into[1] == "VBG" or into[1] == "VBN" or into[0]== "love" or into[0]== "loved" or into[0]== "hate" or into[0]== "hated" or into[0] == "like" into[0] == "liked":
                    sentiment.append(into[0])
                    #se è un avverbio è un modificatore
                if into[1] == "RB":
                    #se l'avverbio è una negazione (contratta o intera) il modificatore (ulteriore) è 'not'
                    if into[0] == "n't" or into[0] == "not":
                        modifier.append("not")
                    else:
                        modifier.append(into[0])
                        
            modifier = ' '.join(modifier)
            sentiment = ' '.join(sentiment)
            
            for feat in feature:
                 if sentiment != []:
                     triple.append((feat, modifier, sentiment))
       
            feature = []
            sentiment = []
            modifier = [] 
            
                
            
        return triple

if __name__=="__main__": 
    print(getTriples("I don't really like coffee"))   
    print(getTriples("I didn't really liked breakfast "))  
    print(getTriples("breakfast was good "))   
    print(getTriples("I liked breakfast and staff was nice "))   
    print(getTriples("I hated food"))  
    print(getTriples("food was very bad")) 
    
 
    
    
    