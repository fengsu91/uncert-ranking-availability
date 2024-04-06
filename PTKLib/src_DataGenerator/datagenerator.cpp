//----------------------------------------------------------------------------------------------------/
// Output file format:
//    = "tuple_argv1_argv2_argv3_argv4_argv5_argv6_argv7_argv8.txt":
//      The output file that contains the tuple information. Each row is in the format <tid, tscore, tprob>
//      tid: id of the tuple; tscore: the ranking score of the tuple; tprob: the membership probability of the tuple.
//      Tuples are in the ranking order.
//    = "rule_argv1_argv2_argv3_argv4_argv5_argv6_argv7_argv8.txt":
//      The output file that contains the rule information. Each row is in the format <rid, tid>
//      (rid: id of the rule; tid: id of the tuple. The tuples in each rule are in the ranking order.)
//----------------------------------------------------------------------------------------------------/

#include <iostream.h>
#include <fstream.h>
#include <vector>
#include <math.h>
#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <algorithm>

#define PI 3.14159265
#define RAND_MAX 32767 

typedef ::std::vector<int> intvector;
typedef ::std::vector<float> floatvector;
typedef ::std::vector<floatvector> floatmatrix;
typedef ::std::vector<intvector> intmatrix;

/*********************************************************************************************************/
// global parameters
/*********************************************************************************************************/
ofstream fout1;
ofstream fout2;
int tupleNum;
int ruleNum;
int rulesize_mu;
int rulesize_sigma;
float tuple_prob_mu;
float tuple_prob_sigma;
float rule_prob_mu;
float rule_prob_sigma;
int max_span;

/*********************************************************************************************************/
// method declaration
/*********************************************************************************************************/
double Normal(double x,double miu,double sigma);
double NormalRandom(double miu,double sigma,double min,double max);
double AverageRandom(double min,double max);
bool int_greater(const int &left,const int& right){return left<right;};
bool float_greater(const float &left,const float& right){return left<right;};

/*********************************************************************************************************/
// main 
/*********************************************************************************************************/
void main(int argc, char* argv[])
{
    tupleNum=atoi(argv[1]);			//parameter[1]: number of tuples in the data set
	ruleNum=atoi(argv[2]);			//parameter[2]: number of rules in the data set

    tuple_prob_mu=atof(argv[3]);	//parameter[3]: expectation of the membership probability of each tuple
    tuple_prob_sigma=atof(argv[4]);	//parameter[4]: variance of the membership probability of each tuple
    
	rule_prob_mu=atof(argv[5]);		//parameter[5]: expectation of the membership probability of each rule
    rule_prob_sigma=atof(argv[6]);	//parameter[6]: variance of the membership probability of each rule    
    
	rulesize_mu=atoi(argv[7]);		//parameter[7]: expectation of the size of the rule (number of tuples in a rule)
    rulesize_sigma=atoi(argv[8]);	//parameter[8]: variance of the size of the rule (number of tuples in a rule)

    char fname1[256];
    char fname2[256];
    strcpy(fname1,"data\\tuple_");
    strcpy(fname2,"data\\rule_");

    strcat(fname1,argv[1]);
    strcat(fname1,"_"); 
    strcat(fname1,argv[2]);
    strcat(fname1,"_");
    strcat(fname1,argv[3]);
    strcat(fname1,"_");
    strcat(fname1,argv[4]);
    strcat(fname1,"_");
    strcat(fname1,argv[5]);
    strcat(fname1,"_");
    strcat(fname1,argv[6]);
    strcat(fname1,"_");
    strcat(fname1,argv[7]);
    strcat(fname1,"_");    
    strcat(fname1,argv[8]);
    strcat(fname1,".txt");

    strcat(fname2,argv[1]);
    strcat(fname2,"_"); 
    strcat(fname2,argv[2]);
    strcat(fname2,"_");
    strcat(fname2,argv[3]);
    strcat(fname2,"_");
    strcat(fname2,argv[4]);
    strcat(fname2,"_");
    strcat(fname2,argv[5]);
    strcat(fname2,"_");
    strcat(fname2,argv[6]);
    strcat(fname2,"_");
    strcat(fname2,argv[7]);
    strcat(fname2,"_");   
    strcat(fname2,argv[8]);
    strcat(fname2,".txt");

    fout1.open(fname1);
    fout2.open(fname2);

    srand((unsigned)time(NULL));

    double random;
    int id;

    floatvector problist;

    for(int i=0;i<tupleNum;i++)
    {       
        //random=(double)rand()/(double)RAND_MAX;   //uniform distribution
        random=NormalRandom(tuple_prob_mu,tuple_prob_sigma,0,1);        //normal distribution        

        problist.push_back(random);
    }

    intvector tuplelabel;
    for(int x=0;x<tupleNum;x++)
    {
        tuplelabel.push_back(x);
    }

    int ruledtuple=0;
    for(i=0;i<ruleNum;i++)
    {
        
        int size=NormalRandom(rulesize_mu,rulesize_sigma,2,rulesize_mu+3*rulesize_sigma);   //generate the rule size    

        ruledtuple+=size;
        if(ruledtuple>tupleNum)
        {
            cout<<"too many tuples in generation rules!"<<endl;
            return;
        }

        intvector tuples;
        for(int j=0;j<size;j++)
        {            
            if(tuplelabel.size()==0)
            {
                cout<<"too many tuples in generation rules!"<<endl;
                return;
            }

            random=(double)rand()/(double)RAND_MAX;
            id=random*tuplelabel.size();
            
			if(id==tuplelabel.size())
            {                
                id--;
            }
            
			tuples.push_back(tuplelabel[id]);
            
			tuplelabel.erase(tuplelabel.begin()+id);            
        }
        
		std::sort(tuples.begin(),tuples.end(),int_greater);

        float ruleprob=NormalRandom(rule_prob_mu,rule_prob_sigma,0,1);

        floatvector ruleprobs;
		float probsum=0;
        for(j=0;j<tuples.size();j++)
        {
            random=(double)rand()/(double)RAND_MAX;
            ruleprobs.push_back(random);
			probsum+=random;
        }        

        for(j=0;j<tuples.size();j++)
        {
			problist[tuples[j]]=(ruleprobs[j]/probsum)*ruleprob;
            
            fout2<<i<<"\t"<<tuples[j];

            if(i<ruleNum-1 || j<tuples.size()-1)
                fout2<<endl;
        }
    }

    for(i=0;i<problist.size();i++)
    {
        fout1<<i<<"\t"<<i<<"\t"<<problist[i];
        if(i<problist.size()-1)
            fout1<<endl;
    }
    
	cout<<"Data generated!"<<endl;

}

/*********************************************************************************************************/
// methods
/*********************************************************************************************************/
//-----------------------------------------------------------------------------------------//zz
double Normal(double x,double miu,double sigma)
{
    return (1.0/(sqrt(2*PI)*sigma)) * exp(-1*(x-miu)*(x-miu)/(2*sigma*sigma));
}
double NormalRandom(double miu,double sigma,double min,double max)
{
    double x;
    double dScope;
    double y;
    do
    {
        x = AverageRandom(min,max); 
        y = Normal(x, miu, sigma);
        dScope = AverageRandom(0, Normal(miu,miu,sigma));
    }while(dScope>y);
    return x;
}
double AverageRandom(double min,double max)
{
    int a;   
    double r;       
    a=rand()%RAND_MAX;   
    r=(a+0.00)/(double)RAND_MAX; 
    double result;
    result=min+r*(max-min);
    return result;  
}
